"""
Utils used in project.
"""
import inspect
import logging
from typing import Callable, Optional

from oss2.compat import to_bytes
from oss2.exceptions import ClientError, InconsistentError
from oss2.utils import Crc64, _get_data_size

from aiooss2.adapter import (
    AsyncIterableAdapter,
    AsyncSizedAdapter,
    AsyncUnsizedAdapter,
    StreamAdapter,
    SyncIterableAdapter,
    SyncSizedAdapter,
    SyncUnsizedAdapter,
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
) -> StreamAdapter:
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
        if hasattr(data, "read") and inspect.iscoroutinefunction(data.read):
            return AsyncSizedAdapter(
                data,
                progress_callback=progress_callback,
                size=size,
                crc_callback=crc_callback,
            )

        wrapped_data = SyncSizedAdapter(
            data,
            progress_callback=progress_callback,
            size=size,
            crc_callback=crc_callback,
        )
    elif hasattr(data, "read"):
        if inspect.iscoroutinefunction(data.read):
            wrapped_data = AsyncUnsizedAdapter(
                data,
                progress_callback=progress_callback,
                discard=discard,
                crc_callback=crc_callback,
            )
        wrapped_data = SyncUnsizedAdapter(
            data,
            progress_callback=progress_callback,
            discard=discard,
            crc_callback=crc_callback,
        )
    elif hasattr(data, "__aiter__"):
        if discard and enable_crc:
            raise ClientError(
                "Iterator adapter does not support discard bytes"
            )
        wrapped_data = AsyncIterableAdapter(
            data,
            progress_callback=progress_callback,
            crc_callback=crc_callback,
        )
    elif hasattr(data, "__iter__"):
        if discard and enable_crc:
            raise ClientError(
                "Iterator adapter does not support discard bytes"
            )
        wrapped_data = SyncIterableAdapter(
            data,
            progress_callback=progress_callback,
            crc_callback=crc_callback,
        )
    else:
        raise ClientError(
            f"{data.__class__.__name__} is not a file object, nor an iterator"
        )
    return wrapped_data
