"""
Adapter (crc check and progress bar call backs) for data types
"""
# pylint: disable=unnecessary-dunder-call
import inspect
import logging
import os
from abc import ABC, abstractmethod
from typing import IO, AsyncIterator, Callable, Optional, Union

from aiohttp.abc import AbstractStreamWriter
from aiohttp.payload import PAYLOAD_REGISTRY, TOO_LARGE_BYTES_BODY, Payload
from oss2.compat import to_bytes
from oss2.utils import (
    _CHUNK_SIZE,
    _invoke_cipher_callback,
    _invoke_crc_callback,
    _invoke_progress_callback,
)

logger = logging.getLogger(__name__)


class StreamAdapter(ABC):
    """Adapter for data types"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        stream: Union[bytes, IO],
        progress_callback: Optional[Callable] = None,
        crc_callback: Optional[Callable] = None,
        cipher_callback: Optional[Callable] = None,
        size: Optional[int] = None,
        discard: int = 0,
    ):
        """
        Args:
            stream (Union[bytes, IO]):
                original data stream can be bytes or file like object.
            progress_callback (Optional[Callable], optional):
                function used for progress bar.
            crc_callback (Optional[Callable], optional):
                function used for crc calculation.
            cipher_callback (Optional[Callable], optional):
                function used for cipher calculation.
            size: Optional[int] = None,
            discard (int): bytes to discard.
        """
        self.stream = to_bytes(stream)
        self.progress_callback: Optional[Callable] = progress_callback
        self.crc_callback: Optional[Callable] = crc_callback
        self.cipher_callback: Optional[Callable] = cipher_callback
        self.offset = 0
        self.size = size
        self.discard = discard

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> bytes:
        content = await self.read(_CHUNK_SIZE)
        if content:
            return content
        raise StopAsyncIteration

    def __len__(self) -> Optional[int]:
        return self.size

    @abstractmethod
    async def read(self, amt=-1) -> bytes:
        """async api to read a chunk from the data

        Args:
            amt (int): batch size of the data to read, -1 to read all
        """

    @property
    def crc(self):
        """return crc value of the data"""
        if self.crc_callback:
            return self.crc_callback.crc
        return None

    def _invoke_callbacks(self, content: Union[str, bytes]):
        content = to_bytes(content)
        self.offset += len(content)

        real_discard = 0
        if self.offset < self.discard:
            real_discard = (
                len(content) if len(content) <= self.discard else self.discard
            )
        self.discard -= real_discard

        _invoke_progress_callback(self.progress_callback, self.offset, self.size)
        _invoke_crc_callback(self.crc_callback, content, real_discard)
        content = _invoke_cipher_callback(self.cipher_callback, content, real_discard)

        return content


class SliceableAdapter(StreamAdapter):
    """Adapter for data can get a slice via `stream[a:b]`"""

    def __init__(
        self,
        stream: Union[bytes, bytearray, memoryview],
        **kwargs,
    ):
        """
        Args:
            size (Optional[int]):
                size of the data stream.
        """
        super().__init__(stream, **kwargs)
        self.size: int = self.size or len(stream)

    def _length_to_read(self, amt: int) -> int:
        length_to_read = self.size - self.offset
        if amt > 0:
            length_to_read = min(amt, length_to_read)
        return length_to_read

    async def read(self, amt: int = -1) -> bytes:
        if self.offset >= self.size:
            return b""
        length_to_read = self._length_to_read(amt)
        content = self.stream[self.offset : self.offset + length_to_read]
        return self._invoke_callbacks(content)


class FilelikeObjectAdapter(StreamAdapter):
    """Adapter for file like data objects"""

    def __init__(self, stream, **kwargs):
        super().__init__(stream, **kwargs)
        self._read_all = False
        if self.size is None:
            try:
                position = self.stream.tell()
                end = self.stream.seek(0, os.SEEK_END)
                self.stream.seek(position)
                self.size = end - position
            except AttributeError:
                pass

    def _length_to_read(self, amt: int) -> int:
        if self.size is None:
            return amt
        length_to_read = self.size - self.offset
        if amt > 0:
            length_to_read = min(amt, length_to_read)
        return length_to_read

    async def read(self, amt: int = -1) -> bytes:
        if self._read_all or (self.size and self.offset >= self.size):
            return b""
        if self.offset < self.discard and amt:
            amt += self.discard - self.offset

        length_to_read = self._length_to_read(amt)

        if inspect.iscoroutinefunction(self.stream.read):
            content = await self.stream.read(length_to_read)
        else:
            content = self.stream.read(length_to_read)
        if not content:
            self._read_all = True
        return self._invoke_callbacks(content)


class IterableAdapter(StreamAdapter):
    """Adapter for Async Iterable data"""

    def __init__(self, stream, discard: int = 0, **kwargs):
        if hasattr(stream, "__aiter__"):
            stream = stream.__aiter__()
        elif hasattr(stream, "__iter__"):
            stream = iter(stream)
        super().__init__(stream, **kwargs)
        if discard:
            raise ValueError(
                "discard not supported in Async " f"Iterable input {self.stream}"
            )

    async def read(self, amt: int = -1) -> bytes:
        try:
            if hasattr(self.stream, "__anext__"):
                content = await self.stream.__anext__()
            elif hasattr(self.stream, "__next__"):
                content = next(self.stream)
            else:
                raise AttributeError(
                    f"{self.stream.__class__.__name__} is neither"
                    " an iterator nor an async iterator"
                )
        except (StopIteration, StopAsyncIteration):
            return b""
        return self._invoke_callbacks(content)


class AsyncPayload(Payload):
    """Payload of async data of unknown length"""

    _value: StreamAdapter

    async def write(self, writer: AbstractStreamWriter) -> None:
        chunk = await self._value.read()
        while chunk:
            if len(chunk) > TOO_LARGE_BYTES_BODY:
                logger.warning(
                    "Sending a large body directly with raw bytes might"
                    " lock the event loop. You should probably pass an "
                    "io.BytesIO object instead.",
                )
            await writer.write(chunk)
            chunk = await self._value.read()


PAYLOAD_REGISTRY.register(
    AsyncPayload, (SliceableAdapter, FilelikeObjectAdapter, IterableAdapter)
)
