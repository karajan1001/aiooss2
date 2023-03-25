"""
Module for exceptions classes used in aiooss2
"""

from oss2.exceptions import _OSS_ERROR_TO_EXCEPTION
from oss2.exceptions import InvalidEncryptionRequest as _InvalidEncryptionRequest
from oss2.exceptions import ServerError, _parse_error_body


class InvalidEncryptionRequest(_InvalidEncryptionRequest):
    """Invalid encryption request error class."""

    def __init__(self, message, details):
        super().__init__(
            self.status,
            {},
            f"InconsistentError: {message}",
            {"details": details},
        )

    def __str__(self):
        return self._str_with_body()


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
