"""
Utils used in project.
"""
from oss2.exceptions import InconsistentError


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
