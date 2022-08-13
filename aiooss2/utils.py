"""
Utils used in project.
"""
import logging
from typing import Any, Callable, Optional

from aiohttp.abc import AbstractStreamWriter
from aiohttp.payload import PAYLOAD_REGISTRY, TOO_LARGE_BYTES_BODY, Payload
from oss2.compat import to_bytes
from oss2.exceptions import ClientError, InconsistentError
from oss2.utils import (
    Crc64,
    _BytesAndFileAdapter,
    _FileLikeAdapter,
    _get_data_size,
    _invoke_cipher_callback,
    _invoke_crc_callback,
    _invoke_progress_callback,
    _IterableAdapter,
)

logger = logging.getLogger(__name__)


async def copyfileobj_and_verify(
    fsrc, fdst, expected_len, chunk_size=16 * 1024, request_id=""
):
    """copy data from file-like object fsrc to file-like object fdst,
    and verify length
    """

    num_read = 0

    while 1:
        buf = await fsrc.read(chunk_size)
        if not buf:
            break

        num_read += len(buf)
        fdst.write(buf)

    if num_read != expected_len:
        raise InconsistentError("IncompleteRead from source", request_id)


def make_adapter(  # pylint: disable=too-many-arguments
    data,
    progress_callback: Optional[Callable] = None,
    size: Optional[int] = None,
    enable_crc: bool = False,
    init_crc: int = 0,
    discard: int = 0,
):
    """Add crc calculation or progress bar callback to the data object.

    Args:
        data (_type_): bytes, file object or async iterable
        progress_callback (Optional[Callable], optional):
            progress bar callback function
        size (Optional[int], optional): size of the data
        enable_crc (bool, optional): enable crc check or not
        init_crc (int, optional): init value of the crc check
        discard (int, optional):

    Raises:
        ClientError: _description_

    Returns:
        _type_: _description_
    """

    data = to_bytes(data)

    if not enable_crc and not progress_callback:
        return data

    if size is None:
        size = _get_data_size(data)

    crc_callback = Crc64(init_crc) if enable_crc else None

    if size:
        if discard and enable_crc:
            raise ClientError(
                "Bytes of file object adapter does not support discard bytes"
            )
        return BytesAndStringAdapter(
            data,
            progress_callback=progress_callback,
            size=size,
            crc_callback=crc_callback,
        )

    if hasattr(data, "read"):
        return FileAdapter(
            data,
            progress_callback,
            crc_callback=crc_callback,
            discard=discard,
        )

    if hasattr(data, "__iter__"):
        if discard and enable_crc:
            raise ClientError(
                "Iterator adapter does not support discard bytes"
            )
        return _IterableAdapter(
            data, progress_callback, crc_callback=crc_callback
        )
    raise ClientError(
        f"{data.__class__.__name__} is not a file object, nor an iterator"
    )


class BytesAndStringAdapter(_BytesAndFileAdapter):
    """Adapter for data with a length attributes"""

    def __len__(self):
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
