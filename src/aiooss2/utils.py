"""
Utils used in project.
"""
import logging
import os
from typing import Callable, Optional

from oss2.compat import to_bytes
from oss2.exceptions import ClientError, InconsistentError
from oss2.utils import Crc64

from aiooss2.adapter import (
    FilelikeObjectAdapter,
    IterableAdapter,
    SliceableAdapter,
    StreamAdapter,
)

COPY_BUFSIZE = 1024 * 1024 if os.name == "nt" else 64 * 1024

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
    stream,
    progress_callback: Optional[Callable] = None,
    enable_crc: bool = False,
    size: Optional[int] = None,
    init_crc: int = 0,
    discard: int = 0,
) -> StreamAdapter:
    """Add crc calculation or progress bar callback to the data object.

    Args:
        stream (_type_): bytes, file object or iterable
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

    stream = to_bytes(stream)

    if not enable_crc and not progress_callback:
        return stream

    crc_callback = Crc64(init_crc) if enable_crc else None

    params = {
        "stream": stream,
        "progress_callback": progress_callback,
        "crc_callback": crc_callback,
        "discard": discard,
        "size": size,
    }

    if isinstance(stream, (bytes, bytearray, memoryview)):
        return SliceableAdapter(**params)
    if hasattr(stream, "read"):
        return FilelikeObjectAdapter(**params)
    if (
        hasattr(stream, "__aiter__")
        or hasattr(stream, "__iter__")
        or hasattr(stream, "__next__")
        or hasattr(stream, "__anext__")
    ):
        return IterableAdapter(**params)
    raise ClientError(
        f"{stream.__class__.__name__} is neither a string nor bytes "
        "nor a file object nor an iterator"
    )


async def copyfileobj(fsrc, fdst, length=0):
    """copy data from file-like object fsrc to file-like object fdst"""
    if not length:
        length = COPY_BUFSIZE
    fsrc_read = fsrc.read
    fdst_write = fdst.write
    while True:
        buf = await fsrc_read(length)
        if not buf:
            break
        fdst_write(buf)
