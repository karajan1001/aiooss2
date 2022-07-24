"""
aiooss2.http
"""
# pylint: disable=invalid-overridden-method
# pylint: disable=too-few-public-methods
import logging
from typing import TYPE_CHECKING, Optional

from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientResponseError
from oss2 import defaults
from oss2.exceptions import RequestError
from oss2.http import _CHUNK_SIZE, Response

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse
    from oss2.http import Request


logger = logging.getLogger(__name__)


class AioResponse(Response):
    """Async Version of the response wrapper adapting to the
    aiooss2 api
    """

    def __init__(self, response: "ClientResponse"):
        response.status_code = response.status  # type: ignore[attr-defined]
        super().__init__(response)
        self.__all_read = False

    async def read(self, amt=None) -> bytes:
        """read the contents from the response"""
        if self.__all_read:
            return b""

        if amt:
            raise NotImplementedError

        content = await self.response.read()
        self.response.release()
        self.__all_read = True
        return content

    async def __iter__(self):
        """content iterator"""
        return await self.response.content.readchunk(_CHUNK_SIZE)

    def release(self):
        """relase the response"""
        self.response.release()


class AioSession:
    """Async session wrapper"""

    def __init__(self, psize: Optional[int] = None):
        """
        Args:
            psize: limit amount of simultaneously opened connections
        """

        self.psize = psize or defaults.connection_pool_size
        self.session: Optional[ClientSession] = None

    async def do_request(
        self, req: "Request", timeout: Optional[int] = None
    ) -> "AioResponse":
        """Do request

        Args:
            req: request info
            timeout: timeout in seconds

        Raises:
            RequestError:

        Returns:
            AioResponse: Async Response wrapper
        """

        logger.debug(
            "Send request, method: %s, url: %s, params: %s, headers: %s, "
            "timeout: %d",
            req.method,
            req.url,
            req.params,
            req.headers,
            timeout,
        )

        try:
            assert self.session
            resp = await self.session.request(
                req.method,
                req.url,
                data=req.data,
                params=req.params,
                headers=req.headers,
                timeout=timeout,
            )
            return AioResponse(resp)
        except ClientResponseError as err:
            raise RequestError(err) from err

    async def __aenter__(self):
        conn = TCPConnector(limit=self.psize, limit_per_host=self.psize)
        self.session = ClientSession(connector=conn)

    async def __aexit__(self, exception_type, exception_value, traceback):
        self.session.close()
