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
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
)

from oss2 import Bucket, defaults, models
from oss2.api import _make_range_string, _normalize_endpoint, _UrlMaker
from oss2.compat import to_string, to_unicode, urlquote
from oss2.exceptions import ClientError, NoSuchKey
from oss2.headers import OSS_COPY_OBJECT_SOURCE
from oss2.http import CaseInsensitiveDict
from oss2.models import (
    AppendObjectResult,
    BatchDeleteObjectsResult,
    GetBucketInfoResult,
    GetObjectMetaResult,
    HeadObjectResult,
    InitMultipartUploadResult,
    ListBucketsResult,
    ListMultipartUploadsResult,
    ListObjectsResult,
    ListPartsResult,
    PutObjectResult,
    RequestResult,
)
from oss2.utils import (
    check_crc,
    content_md5,
    is_valid_bucket_name,
    is_valid_endpoint,
    set_content_type,
)
from oss2.xml_utils import (
    parse_batch_delete_objects,
    parse_dummy_result,
    parse_get_bucket_info,
    parse_list_buckets,
    parse_list_objects,
    to_batch_delete_objects_request,
)

from .exceptions import make_exception
from .http import AioSession, Request
from .models import AioGetObjectResult
from .multipart import (
    abort_multipart_upload,
    complete_multipart_upload,
    init_multipart_upload,
    list_multipart_uploads,
    list_parts,
    upload_part,
    upload_part_copy,
)
from .utils import copyfileobj, copyfileobj_and_verify, make_adapter

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
            endpoint (str): endpoint address or CNAME.
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
                "The endpoint you has specified is not valid, " f"endpoint: {endpoint}"
            )
        self.session = session
        self.timeout = connect_timeout or defaults.connect_timeout
        self.app_name = app_name
        self.enable_crc = enable_crc
        self.proxies = proxies

        self._make_url = _UrlMaker(self.endpoint, is_cname, False)
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
        self.auth._sign_request(  # pylint: disable=protected-access
            req, bucket_name, key
        )

        assert self.session
        resp: "AioResponse" = await self.session.do_request(req, timeout=self.timeout)

        logger.debug(
            "Responses from the server, req_id: %s, status_code: %d",
            resp.request_id,
            resp.status,
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
        resp: "AioResponse" = await self.session.do_request(req, timeout=self.timeout)
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
    async def _parse_result(resp: "AioResponse", parse_func: Callable, klass: Type):
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
    """Used for Bucket and Object operations, creating、deleting Bucket,
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
            raise ClientError(
                f"The bucket_name '{self.bucket_name}' is invalid, please check it."
            )
        super().__init__(
            auth,
            endpoint,
            is_cname,
            **kwargs,
        )

    async def _do_object(
        self, method: str, key: Union[bytes, str], **kwargs
    ) -> "AioResponse":
        return await self._do(method, self.bucket_name, key, **kwargs)

    async def _do_bucket(self, method: str, **kwargs) -> "AioResponse":
        return await self._do(method, self.bucket_name, "", **kwargs)

    async def put_object(
        self,
        key: str,
        data,
        headers: Optional[Mapping] = None,
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
            headers (Optional[Mapping], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.

        Returns:
            PutObjectResult:
        """
        headers = set_content_type(CaseInsensitiveDict(headers), key)

        data = make_adapter(
            data,
            progress_callback=progress_callback,
            enable_crc=self.enable_crc,
        )

        resp: "AioResponse" = await self._do_object(
            "PUT", key, data=data, headers=headers
        )
        logger.debug("Put object done")
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
        params: Optional[Union[Dict, CaseInsensitiveDict]] = None,
    ) -> AioGetObjectResult:
        """download the contents of an object

        (use case) ::
            >>> async with await bucket.get_object("helloword") as result
            >>>     print(await result.read())
            'hello world'

        Args:
            key (str): object name to download.
            byte_range (Optional[Sequence[Optional[int]]], optional):
                Range to download.
            headers (Optional[dict], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.
            process (_type_, optional): oss file process method.
            params (Optional[Union[Dict, CaseInsensitiveDict]], optional):

        Returns:
            AioGetObjectResult:
        """

        headers_dict: CaseInsensitiveDict = CaseInsensitiveDict(headers)

        range_string = _make_range_string(byte_range)
        if range_string:
            headers_dict["range"] = range_string

        params = {} if params is None else params
        if process:
            params[Bucket.PROCESS] = process

        resp = await self._do_object("GET", key, headers=headers_dict, params=params)
        logger.debug("Get object done")

        return AioGetObjectResult(resp, progress_callback, self.enable_crc)

    async def delete_object(
        self,
        key: str,
        params: Union[Dict, CaseInsensitiveDict] = None,
        headers: Optional[Dict] = None,
    ) -> "RequestResult":
        """delete an object

        (use case) ::
            >>> async for obj in AioObjectIterator(bucket):
            >>>    print(obj.key)
            'object1'
            'object2'
            >>> await bucket.delete_object("object1")
            >>> async for obj in AioObjectIterator(bucket):
            >>>    print(obj.key)
            'object2'

        Args:
            key (str): _description_
            headers (Optional[Dict], optional): HTTP headers to specify.
            params (Union[Dict, CaseInsensitiveDict], optional):

        Returns:
            RequestResult:
        """

        resp = await self._do_object("DELETE", key, params=params, headers=headers)
        logger.debug("Delete object done")
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

        (use case) ::
            >>> async for obj in AioObjectIterator(bucket, prefix=object_path):
            >>>    print(obj.key)
            'object1'
            'object2'

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
        resp = await self._do_object(
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
        logger.debug("List objects done")
        return await self._parse_result(resp, parse_list_objects, ListObjectsResult)

    async def get_object_meta(
        self,
        key: str,
        params: Optional[Union[dict, CaseInsensitiveDict]] = None,
        headers: Optional[Dict] = None,
    ) -> "GetObjectMetaResult":
        """get meta data from object

        (use case) ::
            >>> result = await bucket.get_object_meta(object_name)
            >>> print(result.content_length )
            256

        Args:
            key (str): object key
            params (Optional[Union[dict, CaseInsensitiveDict]], optional):
            headers (Optional[Dict], optional): HTTP headers to specify.

        Returns:
            GetObjectMetaResult:
        """
        headers = CaseInsensitiveDict(headers)

        if params is None:
            params = {}

        if Bucket.OBJECTMETA not in params:
            params[Bucket.OBJECTMETA] = ""

        resp = await self._do_object("GET", key, params=params, headers=headers)
        logger.debug("Get object metadata done")
        return GetObjectMetaResult(resp)

    async def object_exists(self, key: str, headers: Optional[Dict] = None) -> bool:
        """Return True if key exists, False otherwise. raise Exception
        for other exceptions.

        (use case) ::
            >>> result = await bucket.object_exists('object1')
            >>> print(result)
            True

        Args:
            key (str): key of the object
            headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.

        Returns:
            bool:
        """

        try:
            await self.get_object_meta(key, headers=headers)
        except NoSuchKey:
            return False

        return True

    async def get_bucket_info(self) -> GetBucketInfoResult:
        """Get bucket information, `Create time`, `Endpoint`, `Owner`, `ACL`

        (use case) ::
            >>> resp = await aiobucket.get_bucket_info()
            >>> print(resp.name)
            'bucket_name'

        Returns:
            GetBucketInfoResult
        """
        resp = await self._do_bucket("GET", params={Bucket.BUCKET_INFO: ""})
        logger.debug("Get bucket info done")
        return await self._parse_result(
            resp, parse_get_bucket_info, GetBucketInfoResult
        )

    async def append_object(
        self,
        key: str,
        position: int,
        data,
        headers: Optional[Dict] = None,
        progress_callback: Optional[Callable] = None,
        init_crc: Optional[int] = None,
    ) -> "AppendObjectResult":
        """Append value to an object

        (use case) ::
            >>> async with await bucket.get_object("object1") as result
            >>>     print(await result.read())
            'hello world'
            >>> resp = await bucket.append_object('object1', 11, "!!!")
            >>> async with await bucket.get_object("object1") as result
            >>>     print(await result.read())
            'hello world!!!'

        Args:
            key (str): key of the object
            position (int): position to append
            data (_type_): data to append
            headers (Optional[Dict], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.
            init_crc (Optional[int], optional): init value of the crc

        Returns:
            AppendObjectResult:
        """
        headers = set_content_type(CaseInsensitiveDict(headers), key)

        data = make_adapter(
            data,
            progress_callback=progress_callback,
            enable_crc=self.enable_crc,
        )

        resp = await self._do_object(
            "POST",
            key,
            data=data,
            headers=headers,
            params={"append": "", "position": str(position)},
        )
        logger.debug("Append object done")
        result = AppendObjectResult(resp)

        if self.enable_crc and result.crc is not None and init_crc is not None:
            check_crc("append object", data.crc, result.crc, result.request_id)

        return result

    async def put_object_from_file(
        self,
        key: str,
        filename: str,
        headers: Optional[Mapping] = None,
        progress_callback: Optional[Callable] = None,
    ) -> "PutObjectResult":
        """Upload a local file to a oss key

        (use case) ::
            >>> with open("file") as f:
            >>>     print(f.read())
            "hello world"
            >>> result = await aiobucket.put_object_from_file("object1",
                "file")
            >>> async with await bucket.get_object("object1") as result
            >>>     print(await result.read())
            'hello world'

        Args:
            key (str): key of the oss
            filename (str): filename to upload
            headers (Optional[Mapping], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.

        Returns:
            _type_: _description_
        """
        headers = set_content_type(CaseInsensitiveDict(headers), filename)
        with open(to_unicode(filename), "rb") as f_stream:
            return await self.put_object(
                key,
                f_stream,
                headers=headers,
                progress_callback=progress_callback,
            )

    async def get_object_to_file(
        self,
        key: str,
        filename: str,
        byte_range: Optional[Sequence[int]] = None,
        headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable] = None,
        process: Optional[Callable] = None,
        params: Optional[Union[Dict, CaseInsensitiveDict]] = None,
    ) -> AioGetObjectResult:
        """Download contents of object to file.

        (use case) ::
            >>> result = await aiobucket.get_object_to_file("object1", "file")
            >>> with open("file") as f:
            >>>     print(f.read())
            "hello world"

        Args:
            key (str): object name to download.
            filename (str): filename to save the data downloaded.
            byte_range (Optional[Sequence[Optional[int]]], optional):
                Range to download.
            headers (Optional[dict], optional): HTTP headers to specify.
            progress_callback (Optional[Callable], optional): callback function
                for progress bar.
            process (_type_, optional): oss file process method.
            params (Optional[Union[Dict, CaseInsensitiveDict]], optional):

        Returns:
            AioGetObjectResult:
        """
        with open(to_unicode(filename), "wb") as f_w:
            result = await self.get_object(
                key,
                byte_range=byte_range,
                headers=headers,
                progress_callback=progress_callback,
                process=process,
                params=params,
            )

            if result.content_length is None:
                copyfileobj(result, f_w)
            else:
                await copyfileobj_and_verify(
                    result,
                    f_w,
                    result.content_length,
                    request_id=result.request_id,
                )

        if self.enable_crc and byte_range is None:
            if (
                (headers is None)
                or ("Accept-Encoding" not in headers)
                or (headers["Accept-Encoding"] != "gzip")
            ):
                check_crc(
                    "get",
                    result.client_crc,
                    result.server_crc,
                    result.request_id,
                )

        return result

    async def batch_delete_objects(
        self, key_list: List[str], headers: Optional[Dict] = None
    ) -> BatchDeleteObjectsResult:
        """Delete a batch of objects

        (use case) ::
            >>> async for obj in AioObjectIterator(bucket, prefix=object_path):
            >>>    print(obj.key)
            'object1'
            'object2'
            'object3'
            'object4'
            'object5'
            'object6'
            >>> await aiobucket.batch_delete_objects(["object1", "object2",
                "object3"])
            >>> async for obj in AioObjectIterator(bucket, prefix=object_path):
            >>>    print(obj.key)
            'object4'
            'object5'
            'object6'

        Args:
            key_list (List[str]): list of objects to delete.
            headers (Optional[Dict], optional): HTTP headers to specify.

        Raises:
            ClientError:

        Returns:
            BatchDeleteObjectResult:
        """
        if not key_list:
            raise ClientError("key_list should not be empty")

        data = to_batch_delete_objects_request(key_list, False)

        header_dict: CaseInsensitiveDict = CaseInsensitiveDict(headers)
        header_dict["Content-MD5"] = content_md5(data)

        resp = await self._do_object(
            "POST",
            "",
            data=data,
            params={"delete": "", "encoding-type": "url"},
            headers=header_dict,
        )
        return await self._parse_result(
            resp, parse_batch_delete_objects, BatchDeleteObjectsResult
        )

    async def copy_object(
        self,
        source_bucket_name: str,
        source_key: str,
        target_key: str,
        headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
        params: Optional[Dict] = None,
    ) -> PutObjectResult:
        """copy object to another place

        (use case) ::
            >>> async for obj in AioObjectIterator(bucket, prefix=object_path):
            >>>    print(obj.key)
            'object1'
            >>> await aiobucket.copy_object(bucket_name, "object1", "object2")
            >>> async for obj in AioObjectIterator(bucket, prefix=object_path):
            >>>    print(obj.key)
            'object1'
            'object2'

        Args:
            source_bucket_name (str): source object bucket
            source_key (str): source object key
            target_key (str): target object key
            headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
                HTTP headers to specify.
            params (Optional[Dict], optional):

        Returns:
            PutObjectResult:
        """

        headers = CaseInsensitiveDict(headers)

        if params and Bucket.VERSIONID in params:
            headers[OSS_COPY_OBJECT_SOURCE] = (
                "/"
                + source_bucket_name
                + "/"
                + urlquote(source_key, "")
                + "?versionId="
                + params[Bucket.VERSIONID]
            )
        else:
            headers[OSS_COPY_OBJECT_SOURCE] = (
                "/" + source_bucket_name + "/" + urlquote(source_key, "")
            )

        resp = await self._do_object("PUT", target_key, headers=headers)

        return PutObjectResult(resp)

    async def head_object(
        self,
        key: str,
        headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
        params: Optional[Mapping] = None,
    ) -> "HeadObjectResult":
        """Get object metadata

            >>> result = bucket.head_object('readme.txt')
            >>> print(result.content_type)
            text/plain

        Args:
            key (str): object key
            headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
                HTTP headers to specify.
            params (Optional[Mapping], optional):

        Returns:
            HeadObjectResult:

        Raises:
            `NotFound <oss2.exceptions.NotFound>` if object does not exist.
        """

        logger.debug(
            "Start to head object, bucket: %s, key: %s, headers: %s",
            self.bucket_name,
            to_string(key),
            headers,
        )

        resp = await self._do_object("HEAD", key, headers=headers, params=params)

        logger.debug(
            "Head object done, req_id: %s, status_code: %s",
            resp.request_id,
            resp.status,
        )
        return await self._parse_result(resp, parse_dummy_result, HeadObjectResult)

    async def abort_multipart_upload(
        self: "AioBucket", *args, **kwargs
    ) -> RequestResult:
        """abort multipart uploading process"""
        return await abort_multipart_upload(self, *args, **kwargs)

    async def complete_multipart_upload(
        self: "AioBucket", *args, **kwargs
    ) -> PutObjectResult:
        """Complete multipart uploading process create a new file."""
        return await complete_multipart_upload(self, *args, **kwargs)

    async def init_multipart_upload(
        self: "AioBucket", *args, **kwargs
    ) -> InitMultipartUploadResult:
        """initialize multipart uploading
        The returning upload id, bucket name and object name together
        defines this uploading event."""
        return await init_multipart_upload(self, *args, **kwargs)

    async def list_multipart_uploads(
        self: "AioBucket", *args, **kwargs
    ) -> ListMultipartUploadsResult:
        """List multipart uploading process"""
        return await list_multipart_uploads(self, *args, **kwargs)

    async def list_parts(self: "AioBucket", *args, **kwargs) -> ListPartsResult:
        """list uploaded parts in a part uploading progress."""
        return await list_parts(self, *args, **kwargs)

    async def upload_part(self: "AioBucket", *args, **kwargs) -> PutObjectResult:
        """upload single part."""
        return await upload_part(self, *args, **kwargs)

    async def upload_part_copy(self: "AioBucket", *args, **kwargs) -> PutObjectResult:
        """copy part or whole of a source file to a slice of a target file."""
        return await upload_part_copy(self, *args, **kwargs)


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
            endpoint (str): endpoint address or CNAME.
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

        (use case) ::
            >>> async for obj in AioBucketIterator(aioservice):
            >>>     print(obj.name)
            'bucket_name1'
            'bucket_name2'
            ...

        Args:
            prefix (str, optional): prefix to filter the buckets results.
            marker (str, optional): paginate separator.
            max_keys (int, optional): max return number per page.
            params (Optional[Dict], optional): Some optional params.

        Returns:
            oss2.models.ListBucketsResult:
        """

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
        logger.debug("List buckets done")
        return await self._parse_result(resp, parse_list_buckets, ListBucketsResult)
