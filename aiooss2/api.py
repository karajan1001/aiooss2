"""
Module for Bucket and Service
"""
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes

import logging
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Optional,
    Sequence,
    Type,
    Union,
)

from oss2 import Bucket, defaults, models
from oss2.api import _make_range_string, _normalize_endpoint, _UrlMaker
from oss2.compat import to_string
from oss2.exceptions import ClientError
from oss2.http import CaseInsensitiveDict, Request
from oss2.models import (
    ListBucketsResult,
    ListObjectsResult,
    PutObjectResult,
    RequestResult,
)
from oss2.utils import (
    check_crc,
    is_valid_bucket_name,
    is_valid_endpoint,
    make_crc_adapter,
    make_progress_adapter,
    set_content_type,
)
from oss2.xml_utils import parse_list_buckets, parse_list_objects

from .exceptions import make_exception
from .http import AioSession
from .models import AioGetObjectResult

if TYPE_CHECKING:
    from oss2 import AnonymousAuth, Auth, StsAuth

    from .http import AioResponse

logger = logging.getLogger(__name__)


class _AioBase:  # pylint: disable=too-few-public-methods
    def __init__(
        self,
        auth: Union["Auth", "AnonymousAuth", "StsAuth"],
        endpoint: str,
        is_cname: bool,
        session: Optional[AioSession] = None,
        connect_timeout: Optional[int] = None,
        app_name: str = "",
        enable_crc: bool = True,
        proxies=None,
    ):
        """_summary_

        Args:
            auth (Union[Auth, AnonymousAuth, StsAuth]): Auth class.
            endpoint (str): enpoint address or CNAME.
            is_cname (bool): Whether the endpoint is a CNAME.
            session (Optional[AioSession], optional): reuse a custom session.
            connect_timeout (int): connection.
            app_name (str, optional): app name.
            enable_crc (bool, optional): enable crc check or not.
            proxies (_type_, optional): proxies settings.

        Raises:
            ClientError: _description_
        """
        self.auth = auth
        self.endpoint = _normalize_endpoint(endpoint.strip())
        if is_valid_endpoint(self.endpoint) is not True:
            raise ClientError(
                "The endpoint you has specified is not valid, "
                f"endpoint: {endpoint}"
            )
        self.session = session
        self.timeout = connect_timeout or defaults.connect_timeout
        self.app_name = app_name
        self.enable_crc = False
        self.proxies = proxies

        self._make_url = _UrlMaker(self.endpoint, is_cname)
        logger.debug(
            "Init endpoint: %s, isCname: %s, connect_timeout: %s, "
            "app_name: %s, enabled_crc: %s, proxies: %s",
            endpoint,
            is_cname,
            connect_timeout,
            app_name,
            enable_crc,
            proxies,
        )

    async def _do(
        self, method: str, bucket_name: str, key: Union[bytes, str], **kwargs
    ) -> "AioResponse":

        key = to_string(key)
        req = Request(
            method,
            self._make_url(bucket_name, key),
            app_name=self.app_name,
            **kwargs,
        )
        req.headers["Content-Type"] = "application/octet-stream"
        self.auth._sign_request(  # pylint: disable=protected-access
            req, bucket_name, key
        )

        if req.headers.get("Accept-Encoding") is None:
            req.headers.pop("Accept-Encoding")

        assert self.session
        resp: "AioResponse" = await self.session.do_request(
            req, timeout=self.timeout
        )
        if resp.status // 100 != 2:
            err = await make_exception(resp)
            logger.info("Exception: %s", err)
            raise err

        content_length = models._hget(  # pylint: disable=protected-access
            resp.headers, "content-length", int
        )
        if content_length is not None and content_length == 0:
            await resp.read()

        return resp

    async def _do_url(self, method, sign_url, **kwargs):
        req = Request(
            method,
            sign_url,
            app_name=self.app_name,
            proxies=self.proxies,
            **kwargs,
        )
        resp: "AioResponse" = await self.session.do_request(
            req, timeout=self.timeout
        )
        if resp.status // 100 != 2:
            err = await make_exception(resp)
            logger.info("Exception: %s", err)
            raise err

        content_length = models._hget(  # pylint: disable=protected-access
            resp.headers, "content-length", int
        )
        if content_length is not None and content_length == 0:
            await resp.read()

        return resp

    @staticmethod
    async def _parse_result(
        resp: "AioResponse", parse_func: Callable, klass: Type
    ):
        result = klass(resp)
        parse_func(result, await resp.read())
        return result

    async def __aenter__(self):
        if self.session is None:
            self.session = AioSession()
        if self.session.closed:
            await self.session.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.session.close()


class AioBucket(_AioBase):
    """Used for Bucket and Object opertions, creating、deleting Bucket,
    uploading、downloading Object, etc。
    use case (bucket in HangZhou area)::

    >>> import oss2
    >>> import aiooss2
    >>> import asyncio
    >>> auth = oss2.Auth('your-access-key-id', 'your-access-key-secret')
    >>> bucket = aiooss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com',
    >>>                         'your-bucket')
    >>> def upload():
    >>>     data = b"\x01" * 1024
    >>>     resp = await bucket.put_object('readme.txt',
    >>>                                    'content of the object')
    >>>     return resp
    >>> loop = asyncio.get_event_loop()
    >>> loop.run_until_complete(upload())
    <oss2.models.PutObjectResult object at 0x029B9930>
    """

    auth: Union["Auth", "AnonymousAuth", "StsAuth"]

    def __init__(
        self,
        auth: Union["Auth", "AnonymousAuth", "StsAuth"],
        endpoint: str,
        bucket_name: str,
        is_cname: bool = False,
        **kwargs,
    ):
        """
        Args:
            bucket_name (str): the bucket name to operate
        """
        self.bucket_name = bucket_name.strip()
        if is_valid_bucket_name(self.bucket_name) is not True:
            raise ClientError("The bucket_name is invalid, please check it.")
        super().__init__(
            auth,
            endpoint,
            is_cname,
            **kwargs,
        )

    async def __do_object(
        self, method: str, key: Union[bytes, str], **kwargs
    ) -> "AioResponse":
        return await self._do(method, self.bucket_name, key, **kwargs)

    async def put_object(
        self,
        key: str,
        data,
        headers: Optional[Dict] = None,
        progress_callback: Optional[Callable] = None,
    ) -> "PutObjectResult":
        """upload some contents to an object

        (use case) ::
            >>> await bucket.put_object('readme.txt', 'content of readme.txt')
            >>> with open(u'local_file.txt', 'rb') as f:
            >>>     await bucket.put_object('remote_file.txt', f)

        Args:
            key (str): object name to upload
            data (Union[str, bytes, IO, Iterable]): contents to upload
            headers (Optional[Dict], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.

        Returns:
            PutObjectResult:
        """
        headers = set_content_type(CaseInsensitiveDict(headers), key)

        if progress_callback:
            data = make_progress_adapter(data, progress_callback)

        if self.enable_crc:
            data = make_crc_adapter(data)

        logger.debug(
            "Start to put object, bucket: %s, key: %s, headers: %s",
            self.bucket_name,
            to_string(key),
            headers,
        )
        resp: "AioResponse" = await self.__do_object(
            "PUT", key, data=data, headers=headers
        )
        logger.debug(
            "Put object done, req_id: %s, status_code: %d",
            resp.request_id,
            resp.status,
        )
        result = PutObjectResult(resp)

        if self.enable_crc and result.crc is not None:
            check_crc("put object", data.crc, result.crc, result.request_id)

        return result

    async def get_object(  # pylint: disable=too-many-arguments
        self,
        key: str,
        byte_range: Optional[Sequence[Optional[int]]] = None,
        headers: Optional[dict] = None,
        progress_callback: Optional[Callable] = None,
        process=None,
        params: Optional[Dict] = None,
    ) -> AioGetObjectResult:
        """download the contents of an object

        (use case) ::
            >>> resp = await bucket.get_object("helloword")
            >>> async with resp as result:
            >>>     data = await result.read()
            >>> print(data)
            'hello world'

        Args:
            key (str): object name to download.
            byte_range (Optional[Sequence[Optional[int]]], optional):
                Range to download.
            headers (Optional[dict], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.
            process (_type_, optional): oss file process method.
            params (Optional[Dict], optional):

        Returns:
            AioGetObjectResult:
        """

        headers_dict: CaseInsensitiveDict = CaseInsensitiveDict(headers)

        range_string = _make_range_string(byte_range)
        if range_string:
            headers_dict["range"] = range_string

        params = {} if params is None else params
        if process:
            params.update({Bucket.PROCESS: process})

        logger.debug(
            "Start to get object, bucket: %s， key: %s,"
            " range: %s, headers: %s, params: %s",
            self.bucket_name,
            to_string(key),
            range_string,
            headers_dict,
            params,
        )
        resp = await self.__do_object(
            "GET", key, headers=headers_dict, params=params
        )
        logger.debug(
            "Get object done, req_id: %s, status_code: %d",
            resp.request_id,
            resp.status,
        )

        return AioGetObjectResult(resp, progress_callback, self.enable_crc)

    async def delete_object(
        self,
        key: str,
        params: Union[Dict, CaseInsensitiveDict] = None,
        headers: Optional[Dict] = None,
    ) -> "RequestResult":
        """delete an object

        Args:
            key (str): _description_
            headers (Optional[Dict], optional): HTTP headers to specify.
            params (Union[Dict, CaseInsensitiveDict], optional):

        Returns:
            RequestResult:
        """

        logger.info(
            "Start to delete object, bucket: %s, key: %s",
            self.bucket_name,
            to_string(key),
        )
        resp = await self.__do_object(
            "DELETE", key, params=params, headers=headers
        )
        logger.debug(
            "Delete object done, req_id: %s, status_code: %d",
            resp.request_id,
            resp.status,
        )
        return RequestResult(resp)

    async def list_objects(  # pylint: disable=too-many-arguments
        self,
        prefix: str = "",
        delimiter: str = "",
        marker: str = "",
        max_keys: int = 100,
        headers: Optional[Dict] = None,
    ) -> "ListObjectsResult":
        """list objects in a bucket

        Args:
            prefix (str, optional): only list objects start with this prefix.
            delimiter (str, optional): delimiter as a folder separator.
            marker (str, optional): use in paginate.
            max_keys (int, optional): numbers of objects for one page.
            headers (Optional[Dict], optional): HTTP headers to specify.

        Returns:
            ListObjectsResult:
        """
        headers = CaseInsensitiveDict(headers)
        logger.debug(
            "Start to List objects, bucket: %s, prefix: %s, delimiter: %s, "
            "marker: %s, max-keys: %d",
            self.bucket_name,
            to_string(prefix),
            delimiter,
            to_string(marker),
            max_keys,
        )
        resp = await self.__do_object(
            "GET",
            "",
            params={
                "prefix": prefix,
                "delimiter": delimiter,
                "marker": marker,
                "max-keys": str(max_keys),
                "encoding-type": "url",
            },
            headers=headers,
        )
        logger.debug(
            "List objects done, req_id: %s, status_code: %d",
            resp.request_id,
            resp.status,
        )
        return await self._parse_result(
            resp, parse_list_objects, ListObjectsResult
        )


# pylint: disable=too-few-public-methods
class AioService(_AioBase):
    """Service class used for operations like list all bucket"""

    def __init__(
        self,
        auth: Union["Auth", "AnonymousAuth", "StsAuth"],
        endpoint: str,
        session: Optional[AioSession] = None,
        connect_timeout: Optional[int] = None,
        app_name: str = "",
        proxies=None,
    ):
        """_summary_

        Args:
            auth (Union[Auth, AnonymousAuth, StsAuth]): Auth class.
            endpoint (str): enpoint address or CNAME.
            session (Optional[AioSession], optional): reuse a custom session.
            connect_timeout (int): connection.
            app_name (str, optional): app name.
            proxies (_type_, optional): proxies settings.
        """
        super().__init__(
            auth,
            endpoint,
            False,
            session,
            connect_timeout,
            app_name=app_name,
            proxies=proxies,
        )

    async def list_buckets(
        self,
        prefix: str = "",
        marker: str = "",
        max_keys: int = 100,
        params: Optional[Dict] = None,
    ) -> ListBucketsResult:
        """List buckets with given prefix of an user

        Args:
            prefix (str, optional): prefix to filter the buckets results.
            marker (str, optional): paginate separator.
            max_keys (int, optional): max return number per page.
            params (Optional[Dict], optional): Some optional params.

        Returns:
            oss2.models.ListBucketsResult:
        """
        logger.debug(
            "Start to list buckets, prefix: %s, marker: %s, max-keys: %d",
            prefix,
            marker,
            max_keys,
        )

        list_param = {}
        list_param["prefix"] = prefix
        list_param["marker"] = marker
        list_param["max-keys"] = str(max_keys)

        if params is not None:
            if "tag-key" in params:
                list_param["tag-key"] = params["tag-key"]
            if "tag-value" in params:
                list_param["tag-value"] = params["tag-value"]

        resp = await self._do("GET", "", "", params=list_param)
        logger.debug(
            "List buckets done, req_id: %s, status_code: %s",
            resp.request_id,
            resp.status,
        )
        return await self._parse_result(
            resp, parse_list_buckets, ListBucketsResult
        )
