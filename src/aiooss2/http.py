"""
aiooss2.http
"""
import logging
from typing import TYPE_CHECKING, Optional

from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientResponseError
from oss2 import defaults
from oss2.exceptions import RequestError
from oss2.http import _CHUNK_SIZE, USER_AGENT, CaseInsensitiveDict

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


logger = logging.getLogger(__name__)


class Request:  # pylint: disable=too-few-public-methods
    """Request class for aiohttp"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        method,
        url,
        data=None,
        params=None,
        headers=None,
        app_name="",
        proxies=None,
    ):
        self.method = method
        self.url = url
        self.data = data
        self.params = params or {}
        self.proxies = proxies

        if not isinstance(headers, CaseInsensitiveDict):
            self.headers = CaseInsensitiveDict(headers)
        else:
            self.headers = headers

        if "Content-Type" not in self.headers:
            self.headers["Content-Type"] = "application/octet-stream"
        if "User-Agent" not in self.headers:
            if app_name:
                self.headers["User-Agent"] = USER_AGENT + "/" + app_name
            else:
                self.headers["User-Agent"] = USER_AGENT

        logger.debug(
            "Init request, method: %s, url: %s, params: %s, headers: %s",
            method,
            url,
            params,
            headers,
        )


class AioResponse:
    """Async Version of the response wrapper adapting to the
    aiooss2 api
    """

    response: "ClientResponse"

    def __init__(self, response: "ClientResponse"):
        self.response = response
        self.status = response.status
        self.headers = response.headers
        self.request_id = response.headers.get("x-oss-request-id", "")
        self.__all_read = False
        logger.debug(
            "Get response headers, req-id: %s, status: %d, headers: %s",
            self.request_id,
            self.status,
            self.headers,
        )

    async def read(self, amt=None) -> bytes:
        """read the contents from the response"""
        if self.__all_read:
            return b""

        if amt:
            async for chunk in self.response.content.iter_chunked(amt):
                return chunk
            self.__all_read = True
            return b""

        content_list = []
        async for chunk in self.response.content.iter_chunked(_CHUNK_SIZE):
            content_list.append(chunk)
        content = b"".join(content_list)

        self.__all_read = True
        return content

    async def __aiter__(self):
        """content iterator"""
        return await self.response.content.iter_chunked(_CHUNK_SIZE)

    def release(self):
        """release the response"""
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
        self.conn: Optional[TCPConnector] = None

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
        self.conn = TCPConnector(limit=self.psize, limit_per_host=self.psize)
        self.session = ClientSession(connector=self.conn)
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        """gracefully close the AioSession class"""
        await self.conn.close()
        await self.session.close()

    @property
    def closed(self):
        """Is client session closed.

        A readonly property.
        """
        if self.session is None:
            return True
        return self.session.closed
