"""
Module for exceptions classes used in aiooss2
"""

from oss2.exceptions import (
    _OSS_ERROR_TO_EXCEPTION,
    ServerError,
    _parse_error_body,
)


async def make_exception(resp):
    """read the body and raise a proper exception"""
    status = resp.status
    headers = resp.headers
    body = await resp.read()
    details = _parse_error_body(body)
    code = details.get("Code", "")

    try:
        klass = _OSS_ERROR_TO_EXCEPTION[(status, code)]
        return klass(status, headers, body, details)
    except KeyError:
        return ServerError(status, headers, body, details)
