"""
Module for all input and output classes for the Python SDK API
"""
from typing import TYPE_CHECKING

from oss2.models import GetObjectResult

if TYPE_CHECKING:
    from .http import AioResponse


class AioGetObjectResult(GetObjectResult):
    """class for the result of api get_object"""

    resp: "AioResponse"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.resp.release()
