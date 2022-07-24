"""
Module for Bucket and Service
"""
# pylint: disable=invalid-overridden-method

import logging
from typing import Optional

from oss2 import Auth, Bucket, models, xml_utils
from oss2.api import _make_range_string
from oss2.compat import to_string
from oss2.http import CaseInsensitiveDict, Request
from oss2.models import (
    GetObjectResult,
    ListObjectsResult,
    PutObjectResult,
    RequestResult,
)
from oss2.utils import (
    check_crc,
    make_crc_adapter,
    make_progress_adapter,
    set_content_type,
)

from .exceptions import make_exception
from .http import AioResponse, AioSession

logger = logging.getLogger(__name__)


class AioBucket(Bucket):
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

    :param auth: 包含了用户认证信息的Auth对象
    :param endpoint: 访问域名或者CNAME
    :param bucket_name: Bucket名
    :param is_cname: 如果endpoint是CNAME则设为True；反之，则为False。
    :param session: 会话。如果是None表示新开会话，非None则复用传入的会话
    :param connect_timeout: 连接超时时间，以秒为单位。
    :param app_name: 应用名。该参数不为空，则在User Agent中加入其值。
        注意到，最终这个字符串是要作为HTTP Header的值传输的，所以必须要遵循HTTP标准。
    """

    auth: Auth

    def __init__(
        self,
        auth,
        endpoint,
        bucket_name,
        session: Optional[AioSession] = None,
        **kwargs
    ):
        super().__init__(
            auth, endpoint, bucket_name, session=session, **kwargs
        )
        self.session: Optional[AioSession] = session
        self.enable_crc: bool = False

    async def __aenter__(self):
        if self.session is None:
            self.session = AioSession()
            await self.session.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.session.__aenter__()

    async def __do_object(self, method, key, **kwargs) -> AioResponse:
        return await self._do(method, self.bucket_name, key, **kwargs)

    async def _do(self, method, bucket_name, key, **kwargs) -> AioResponse:

        key = to_string(key)
        req = Request(
            method,
            self._make_url(bucket_name, key),
            app_name=self.app_name,
            **kwargs
        )
        req.headers["Content-Type"] = "application/octet-stream"
        self.auth._sign_request(  # pylint: disable=protected-access
            req, bucket_name, key
        )

        if req.headers.get("Accept-Encoding") is None:
            req.headers.pop("Accept-Encoding")

        assert self.session
        resp: AioResponse = await self.session.do_request(
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

    async def put_object(
        self, key, data, headers=None, progress_callback=None
    ):
        """上传一个普通文件。

        用法 ::
            >>> bucket.put_object('readme.txt', 'content of readme.txt')
            >>> with open(u'local_file.txt', 'rb') as f:
            >>>     bucket.put_object('remote_file.txt', f)

        :param key: 上传到OSS的文件名

        :param data: 待上传的内容。
        :type data: bytes，str或file-like object

        :param headers: 用户指定的HTTP头部。可以指定Content-Type、Content-MD5、
        x-oss-meta-开头的头部等
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。可以用来实现进度条等功能。
        参考 :ref:`progress_callback` 。

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
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
        resp: AioResponse = await self.__do_object(
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
        byte_range=None,
        headers=None,
        progress_callback=None,
        process=None,
        params=None,
    ):
        """下载一个文件。

        用法 ::

            >>> result = bucket.get_object('readme.txt')
            >>> print(result.read())
            'hello world'

        :param key: 文件名
        :param byte_range: 指定下载范围。参见 :ref:`byte_range`

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。参考 :ref:`progress_callback`

        :param process: oss文件处理，如图像服务等。指定后process，返回的内容为处理后的文件。

        :param params: http 请求的查询字符串参数
        :type params: dict

        :return: file-like object

        :raises: 如果文件不存在，则抛出 :class:`NoSuchKey <oss2.exceptions.NoSuchKey>`
        ；还可能抛出其他异常
        """

        headers = CaseInsensitiveDict(headers)

        range_string = _make_range_string(byte_range)
        if range_string:
            headers["range"] = range_string

        params = {} if params is None else params
        if process:
            params.update({Bucket.PROCESS: process})

        logger.debug(
            "Start to get object, bucket: %s， key: %s,"
            " range: %s, headers: %s, params: %s",
            self.bucket_name,
            to_string(key),
            range_string,
            headers,
            params,
        )
        resp = await self.__do_object(
            "GET", key, headers=headers, params=params
        )
        logger.debug(
            "Get object done, req_id: %s, status_code: %d",
            resp.request_id,
            resp.status,
        )

        return GetObjectResult(resp, progress_callback, self.enable_crc)

    async def delete_object(self, key, params=None, headers=None):
        """删除一个文件。

        :param str key: 文件名
        :param params: 请求参数

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`RequestResult <oss2.models.RequestResult>`
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

    @staticmethod
    async def _parse_result(resp, parse_func, klass):
        result = klass(resp)
        parse_func(result, await resp.read())
        return result

    async def list_objects(  # pylint: disable=too-many-arguments
        self, prefix="", delimiter="", marker="", max_keys=100, headers=None
    ):
        """根据前缀罗列Bucket里的文件。

        :param str prefix: 只罗列文件名为该前缀的文件
        :param str delimiter: 分隔符。可以用来模拟目录
        :param str marker: 分页标志。首次调用传空串，后续使用返回值的next_marker
        :param int max_keys: 最多返回文件的个数，文件和目录的和不能超过该值

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`ListObjectsResult <oss2.models.ListObjectsResult>`
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
            resp, xml_utils.parse_list_objects, ListObjectsResult
        )


# pylint: disable=too-few-public-methods
class AioService:
    """用于Service操作的类，如罗列用户所有的Bucket。

    用法 ::

        >>> import oss2
        >>> auth = oss2.Auth('your-access-key-id', 'your-access-key-secret')
        >>> service = oss2.Service(auth, 'oss-cn-hangzhou.aliyuncs.com')
        >>> service.list_buckets()
        <oss2.models.ListBucketsResult object at 0x0299FAB0>

    :param auth: 包含了用户认证信息的Auth对象
    :type auth: oss2.Auth

    :param str endpoint: 访问域名，如杭州区域的域名为oss-cn-hangzhou.aliyuncs.com

    :param session: 会话。如果是None表示新开会话，非None则复用传入的会话
    :type session: oss2.Session

    :param float connect_timeout: 连接超时时间，以秒为单位。
    :param str app_name: 应用名。该参数不为空，则在User Agent中加入其值。
        注意到，最终这个字符串是要作为HTTP Header的值传输的，所以必须要遵循HTTP标准。
    """
