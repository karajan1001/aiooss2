"""
Adapter (crc check and progress bar call backs) for data types
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import (
    IO,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Optional,
    Union,
)

from aiohttp.abc import AbstractStreamWriter
from aiohttp.payload import PAYLOAD_REGISTRY, TOO_LARGE_BYTES_BODY, Payload
from oss2.compat import to_bytes
from oss2.utils import (
    _CHUNK_SIZE,
    _get_data_size,
    _invoke_cipher_callback,
    _invoke_crc_callback,
    _invoke_progress_callback,
)

logger = logging.getLogger(__name__)


class StreamAdapter:  # pylint: disable=too-few-public-methods
    """Adapter for data types"""

    def __init__(
        self,
        stream: Union[bytes, IO],
        progress_callback: Optional[Callable] = None,
        crc_callback: Optional[Callable] = None,
        cipher_callback: Optional[Callable] = None,
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
        """
        self.stream = to_bytes(stream)
        self.progress_callback: Optional[Callable] = progress_callback
        self.crc_callback: Optional[Callable] = crc_callback
        self.cipher_callback: Optional[Callable] = cipher_callback
        self.offset = 0

    @property
    def crc(self):
        """return crc value of the data"""
        if self.crc_callback:
            return self.crc_callback.crc
        return None


class SyncAdapter(StreamAdapter, ABC):
    """Adapter for data with sync read method"""

    def __iter__(self) -> Iterator:
        return self

    def __next__(self) -> bytes:
        content = self.read(_CHUNK_SIZE)
        if content:
            return content
        raise StopIteration

    @abstractmethod
    def read(self, amt=None) -> Awaitable[bytes]:
        """sync api to read a chunk from the data

        Args:
            amt (int, optional): batch size of the data to read
        """


class AsyncAdapter(StreamAdapter, ABC):
    """Adapter for data with async read method"""

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> Awaitable[bytes]:
        content = await self.read(_CHUNK_SIZE)
        if content:
            return content
        raise StopAsyncIteration

    @abstractmethod
    async def read(self, amt=None) -> Awaitable[bytes]:
        """async api to read a chunk from the data

        Args:
            amt (int, optional): batch size of the data to read
        """


class SizedAdapter(StreamAdapter):
    """Adapter for data that can have a fixed size"""

    def __init__(
        self,
        stream: Union[bytes, str, IO],
        size: Optional[int] = None,
        **kwargs,
    ):
        """
        Args:
            size (Optional[int]):
                size of the data stream.
        """
        self.size = size or _get_data_size(stream)
        super().__init__(stream, **kwargs)

    def __len__(self) -> int:
        return self.size

    def _length_to_read(self, amt: Optional[int]) -> int:
        length_to_read = self.size - self.offset
        if amt and amt > 0:
            length_to_read = min(amt, length_to_read)
        return length_to_read

    def _invoke_callbacks(self, content: bytes):
        self.offset += len(content)
        _invoke_progress_callback(
            self.progress_callback, min(self.offset, self.size), self.size
        )
        if content:
            _invoke_crc_callback(self.crc_callback, content)
            content = _invoke_cipher_callback(self.cipher_callback, content)
        return content


class SyncSizedAdapter(SyncAdapter, SizedAdapter):
    """Adapter for data can get its length via `len(size)`"""

    def read(self, amt: Optional[int] = None) -> bytes:
        if self.offset >= self.size:
            return b""
        length_to_read = self._length_to_read(amt)
        if hasattr(self.stream, "read"):
            content = self.stream.read(length_to_read)
        else:
            content = self.stream[self.offset : self.offset + length_to_read]
        return self._invoke_callbacks(content)


class AsyncSizedAdapter(AsyncAdapter, SizedAdapter):
    """Adapter for file-like object"""

    async def read(self, amt: Optional[int] = None) -> bytes:
        if self.offset >= self.size:
            return b""
        length_to_read = self._length_to_read(amt)
        if hasattr(self.stream, "read"):
            content = await self.stream.read(length_to_read)
        else:
            content = self.stream[self.offset : self.offset + length_to_read]
        return self._invoke_callbacks(content)


class UnsizedAdapter(StreamAdapter):  # pylint: disable=too-few-public-methods
    """Adapter for data that do not know its size."""

    def __init__(
        self, stream: Union[bytes, str, IO], discard: int = 0, **kwargs
    ):
        super().__init__(stream, **kwargs)
        self._read_all = False
        self.discard = discard
        self.size = None

    def _invoke_callbacks(self, content):
        if not content:
            self._read_all = True
        else:

            self.offset += len(content)

            real_discard = 0
            if self.offset < self.discard:
                if len(content) <= self.discard:
                    real_discard = len(content)
                else:
                    real_discard = self.discard

            _invoke_progress_callback(
                self.progress_callback, self.offset, None
            )
            _invoke_crc_callback(self.crc_callback, content, real_discard)
            content = _invoke_cipher_callback(
                self.cipher_callback, content, real_discard
            )

            self.discard -= real_discard
        return content


class SyncUnsizedAdapter(SyncAdapter, UnsizedAdapter):
    """
    Adapter for data that have a `read` method to get its
    data but do not know its size.
    """

    def read(self, amt: Optional[int] = None) -> bytes:
        if self._read_all:
            return b""
        if self.offset < self.discard and amt and self.cipher_callback:
            amt += self.discard

        content = self.stream.read(amt)
        return self._invoke_callbacks(content)


class AsyncUnsizedAdapter(AsyncAdapter, UnsizedAdapter):
    """
    Adapter for data that have a async `read` method to get its
    data but do not know its size.
    """

    async def read(self, amt: Optional[int] = None) -> bytes:
        if self._read_all:
            return b""
        if self.offset < self.discard and amt and self.cipher_callback:
            amt += self.discard

        content = await self.stream.read(amt)
        return self._invoke_callbacks(content)


class IterableAdapterMixin(
    StreamAdapter
):  # pylint: disable=too-few-public-methods
    """Mixin for iterable adapter"""

    def _invoke_callbacks(self, content):
        content = to_bytes(content)
        self.offset += len(content)
        _invoke_progress_callback(self.progress_callback, self.offset, None)
        _invoke_crc_callback(self.crc_callback, content)
        return _invoke_cipher_callback(self.cipher_callback, content)


class SyncIterableAdapter(SyncAdapter, IterableAdapterMixin):
    """Adapter for Iterable  data"""

    def read(self, amt: Optional[int] = None) -> bytes:
        content = next(self.stream)
        return self._invoke_callbacks(content)


class AsyncIterableAdapter(AsyncAdapter, IterableAdapterMixin):
    """Adapter for Async Iterable data"""

    async def read(self, amt: Optional[int] = None) -> Awaitable[bytes]:
        content = await next(self.stream)
        return self._invoke_callbacks(content)


class SizedPayload(Payload, ABC):
    """Payload of data with a fixed length"""

    def __init__(self, value: SizedAdapter, *args: Any, **kwargs: Any) -> None:
        if "content_type" not in kwargs:
            kwargs["content_type"] = "application/octet-stream"

        super().__init__(value, *args, **kwargs)

        self._size = len(value)


class SyncSizedPayload(SizedPayload):
    """Payload of data with a length attributes"""

    _value: SyncSizedAdapter

    def __init__(self, value: SyncSizedAdapter, *args, **kwargs):
        if not isinstance(value, SyncSizedAdapter):
            raise TypeError(
                "value argument must be SyncSizedAdapter"
                f", not {type(value)!r}"
            )
        super().__init__(value, *args, **kwargs)

        if self._size > TOO_LARGE_BYTES_BODY:
            kwargs = {"source": self}
            logger.warning(
                "Sending a large body directly with raw bytes might"
                " lock the event loop. You should probably pass an "
                "io.BytesIO object instead",
                ResourceWarning,
                **kwargs,
            )

    async def write(self, writer: AbstractStreamWriter) -> None:
        await writer.write(self._value.read())


class AsyncSizedPayload(SizedPayload):
    """Payload of data with a length attributes"""

    _value: AsyncSizedAdapter

    def __init__(self, value: AsyncSizedAdapter, *args, **kwargs):
        if not isinstance(value, AsyncSizedAdapter):
            raise TypeError(
                "value argument must be AsyncSizedAdapter"
                f", not {type(value)!r}"
            )
        super().__init__(value, *args, **kwargs)

    async def write(self, writer: AbstractStreamWriter) -> None:
        await writer.write(await self._value.read())


class UnsizedPayload(Payload, ABC):
    """Payload of data of unknown length"""

    def __init__(self, value: SizedAdapter, *args: Any, **kwargs: Any) -> None:
        if "content_type" not in kwargs:
            kwargs["content_type"] = "application/octet-stream"

        super().__init__(value, *args, **kwargs)


class SyncUnsizedPayload(UnsizedPayload):
    """Payload of sync data of unknown length"""

    _value: SyncUnsizedAdapter

    async def write(self, writer: AbstractStreamWriter) -> None:
        loop = asyncio.get_event_loop()
        chunk = await loop.run_in_executor(None, self._value.read, 2**16)
        while chunk:
            await writer.write(chunk)
            chunk = await loop.run_in_executor(None, self._value.read, 2**16)


class AsyncUnsizedPayload(UnsizedPayload):
    """Payload of async data of unknown length"""

    _value: AsyncUnsizedAdapter

    async def write(self, writer: AbstractStreamWriter) -> None:
        chunk = await self._value.read(2**16)
        while chunk:
            await writer.write(chunk)
            chunk = await self._value.read(2**16)


class SyncIterablePayload(UnsizedPayload):
    """Payload of async data of unknown length"""

    _value: SyncIterableAdapter

    async def write(self, writer: AbstractStreamWriter) -> None:
        chunk = self._value.read()
        while chunk:
            await writer.write(chunk)
            chunk = self._value.read()


class AsyncIterablePayload(UnsizedPayload):
    """Payload of async data of unknown length"""

    _value: AsyncIterableAdapter

    async def write(self, writer: AbstractStreamWriter) -> None:
        chunk = await self._value.read()
        while chunk:
            await writer.write(chunk)
            chunk = await self._value.read()


PAYLOAD_REGISTRY.register(SyncSizedPayload, SyncSizedAdapter)
PAYLOAD_REGISTRY.register(AsyncSizedPayload, AsyncSizedAdapter)
PAYLOAD_REGISTRY.register(SyncUnsizedPayload, SyncUnsizedAdapter)
PAYLOAD_REGISTRY.register(AsyncUnsizedPayload, AsyncUnsizedAdapter)
PAYLOAD_REGISTRY.register(SyncIterablePayload, SyncIterableAdapter)
PAYLOAD_REGISTRY.register(AsyncIterablePayload, AsyncIterableAdapter)
