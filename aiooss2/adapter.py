"""
Adapter (crc check and progress bar call backs) for data types
"""
import logging
from typing import Any, Optional

from aiohttp.abc import AbstractStreamWriter
from aiohttp.payload import PAYLOAD_REGISTRY, TOO_LARGE_BYTES_BODY, Payload
from oss2.compat import to_bytes
from oss2.utils import (
    _BytesAndFileAdapter,
    _FileLikeAdapter,
    _invoke_cipher_callback,
    _invoke_crc_callback,
    _invoke_progress_callback,
)

logger = logging.getLogger(__name__)


class BytesAndStringAdapter(_BytesAndFileAdapter):
    """Adapter for data with a length attributes"""

    def __len__(self) -> int:
        return self.size

    async def aread(self, amt: Optional[int] = None):
        """read data

        Args:
            amt (int, optional): batch size of the data to read
        """
        if self.offset >= self.size:
            return to_bytes("")

        if amt is None or amt < 0:
            bytes_to_read = self.size - self.offset
        else:
            bytes_to_read = min(amt, self.size - self.offset)

        content = await self.data.read(bytes_to_read)

        self.offset += bytes_to_read

        _invoke_progress_callback(
            self.progress_callback, min(self.offset, self.size), self.size
        )

        _invoke_crc_callback(self.crc_callback, content)

        content = _invoke_cipher_callback(self.cipher_callback, content)

        return content


class BytesOrStringPayload(Payload):
    """Payload of data with a length attributes"""

    _value: BytesAndStringAdapter

    def __init__(
        self, value: BytesAndStringAdapter, *args: Any, **kwargs: Any
    ) -> None:
        if not isinstance(value, BytesAndStringAdapter):
            raise TypeError(
                "value argument must be aiooss2.utils.BytesAndStringAdapter"
                f", not {type(value)!r}"
            )

        if "content_type" not in kwargs:
            kwargs["content_type"] = "application/octet-stream"

        super().__init__(value, *args, **kwargs)

        self._size = len(value)

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
        """payload data writer

        Args:
            writer (AbstractStreamWriter): _description_
        """
        await writer.write(self._value.read())


class FileAdapter(_FileLikeAdapter):
    """adapter for those data do not know its size."""

    async def aread(self, amt=None):
        """async read read from the fileobj

        Args:
            amt (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        offset_start = self.offset
        if offset_start < self.discard and amt and self.cipher_callback:
            amt += self.discard

        content = await self.fileobj.read(amt)
        if not content:
            self.read_all = True
            _invoke_progress_callback(
                self.progress_callback, self.offset, None
            )
        else:
            _invoke_progress_callback(
                self.progress_callback, self.offset, None
            )

            self.offset += len(content)

            real_discard = 0
            if offset_start < self.discard:
                if len(content) <= self.discard:
                    real_discard = len(content)
                else:
                    real_discard = self.discard

            _invoke_crc_callback(self.crc_callback, content, real_discard)
            content = _invoke_cipher_callback(
                self.cipher_callback, content, real_discard
            )

            self.discard -= real_discard
        return content


PAYLOAD_REGISTRY.register(BytesOrStringPayload, _BytesAndFileAdapter)
