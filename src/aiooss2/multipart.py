"""
Module for multipart operations
"""
# pylint: disable=protected-access
# pylint: disable=too-many-arguments

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Union

from oss2.api import Bucket, _make_range_string
from oss2.compat import urlquote
from oss2.headers import OSS_COPY_OBJECT_SOURCE, OSS_COPY_OBJECT_SOURCE_RANGE
from oss2.http import CaseInsensitiveDict
from oss2.models import (
    InitMultipartUploadResult,
    ListMultipartUploadsResult,
    ListPartsResult,
    PutObjectResult,
    RequestResult,
)
from oss2.utils import calc_obj_crc_from_parts, check_crc, set_content_type
from oss2.xml_utils import (
    parse_init_multipart_upload,
    parse_list_multipart_uploads,
    parse_list_parts,
    to_complete_upload_request,
)

from .utils import make_adapter

if TYPE_CHECKING:
    from oss2.models import PartInfo

    from aiooss2.api import AioBucket


logger = logging.getLogger(__name__)


async def init_multipart_upload(
    self: "AioBucket",
    key: str,
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
    params: Optional[Dict] = None,
) -> InitMultipartUploadResult:
    """initialize multipart uploading
    The returning upload id, bucket name and object name together
    defines this uploading event.

    Args:
        key (str): key value of the object
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.
        params (Optional[Dict], optional):

    Returns:
        InitMultipartUploadResult:
    """
    headers = set_content_type(CaseInsensitiveDict(headers), key)

    if params is None:
        tmp_params = {}
    else:
        tmp_params = params.copy()

    tmp_params["uploads"] = ""
    logger.debug("Start to init multipart upload")
    resp = await self._do_object("POST", key, params=tmp_params, headers=headers)
    logger.debug("Init multipart upload done")
    return await self._parse_result(
        resp, parse_init_multipart_upload, InitMultipartUploadResult
    )


async def upload_part(
    self: "AioBucket",
    key: str,
    upload_id: str,
    part_number: int,
    data: Any,
    progress_callback: Optional[Callable] = None,
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
) -> PutObjectResult:
    """upload one part

    Args:
        key (str): key value of the object
        upload_id (str): the upload event id
        part_number (int): the part number of this upload
        data (Any): contents to upload
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.
        progress_callback (Optional[Callable], optional): callback function
            for progress bar.

    Returns:
        PutObjectResult: PutObjectResult
    """

    headers = CaseInsensitiveDict(headers)

    data = make_adapter(
        data,
        progress_callback=progress_callback,
        enable_crc=self.enable_crc,
    )

    logger.debug("Start to upload multipart")

    resp = await self._do_object(
        "PUT",
        key,
        params={"uploadId": upload_id, "partNumber": str(part_number)},
        headers=headers,
        data=data,
    )
    logger.debug("Upload multipart done")
    result = PutObjectResult(resp)

    if self.enable_crc and result.crc is not None:
        check_crc("upload part", data.crc, result.crc, result.request_id)

    return result


async def complete_multipart_upload(
    self: "AioBucket",
    key: str,
    upload_id: str,
    parts: List["PartInfo"],
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
) -> PutObjectResult:
    """Complete multipart uploading process create a new file.

    Args:
        key (str): key value of the object
        upload_id (str): the upload event id
        parts (List[PartInfo]): list of PathInfo
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.

    Returns:
        PutObjectResult:
    """

    headers = CaseInsensitiveDict(headers)

    parts = sorted(parts, key=lambda p: p.part_number)
    data = to_complete_upload_request(parts)

    logger.debug("Start to complete multipart upload")

    resp = await self._do_object(
        "POST",
        key,
        params={"uploadId": upload_id},
        data=data,
        headers=headers,
    )
    logger.debug("Complete multipart upload %s done.", upload_id)

    result = PutObjectResult(resp)

    if self.enable_crc and parts is not None:
        object_crc = calc_obj_crc_from_parts(parts)
        check_crc("multipart upload", object_crc, result.crc, result.request_id)

    return result


async def abort_multipart_upload(
    self: "AioBucket",
    key: str,
    upload_id: str,
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
) -> RequestResult:
    """abort multipart uploading process

    Args:
        key (str): key value of the object
        upload_id (str): the upload event id
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.

    Returns:
        RequestResult:
    """

    logger.debug("Start to abort multipart upload")

    headers = CaseInsensitiveDict(headers)

    resp = await self._do_object(
        "DELETE", key, params={"uploadId": upload_id}, headers=headers
    )
    logger.debug("Abort multipart done")
    return RequestResult(resp)


async def list_multipart_uploads(
    self: "AioBucket",
    prefix: str = "",
    delimiter: str = "",
    key_marker: str = "",
    upload_id_marker: str = "",
    max_uploads: int = 1000,
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
) -> ListMultipartUploadsResult:
    """List multipart uploading process

    Args:
        prefix (str, optional): Only list the parts with this prefix.
        delimiter (str, optional): Directory delimiter.
        key_marker (str, optional): Filename for paging. Do not required
        in the first call of this function. Set to `next_key_marker`
        from result in the following calls.
        upload_id_marker (str, optional): paging separator, Do not required
        in the first call of this function. Set to `next_upload_id_marker`
        from result in the following calls.
        max_uploads (int, optional): max return numbers per page.
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.

    Returns:
        ListMultipartUploadsResult:
    """
    logger.debug("Start to list multipart uploads")

    headers = CaseInsensitiveDict(headers)

    resp = await self._do_object(
        "GET",
        "",
        params={
            "uploads": "",
            "prefix": prefix,
            "delimiter": delimiter,
            "key-marker": key_marker,
            "upload-id-marker": upload_id_marker,
            "max-uploads": str(max_uploads),
            "encoding-type": "url",
        },
        headers=headers,
    )
    logger.debug("List multipart uploads done, req_id")
    return await self._parse_result(
        resp,
        parse_list_multipart_uploads,
        ListMultipartUploadsResult,
    )


async def upload_part_copy(
    self: "AioBucket",
    source_bucket_name: str,
    source_key: str,
    byte_range: Sequence[int],
    target_key: str,
    target_upload_id: str,
    target_part_number: int,
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
    params: Optional[Dict] = None,
) -> PutObjectResult:
    """copy part or whole of a source file to a slice of a target file.

    Args:
        source_bucket_name (str): bucket name of the source file
        source_key (str): key of the source file
        byte_range (Sequence[int]): range of the source file to copy
        target_key (str): key of the target file
        target_upload_id (str): upload id of the target file
        target_part_number (int): part number of the target file
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.
        params (Optional[Dict], optional):

    Returns:
        PutObjectResult:
    """
    headers = CaseInsensitiveDict(headers)

    parameters: Dict[str, Any] = params or {}

    if Bucket.VERSIONID in parameters:
        headers[OSS_COPY_OBJECT_SOURCE] = (
            "/"
            + source_bucket_name
            + "/"
            + urlquote(source_key, "")
            + "?versionId="
            + parameters[Bucket.VERSIONID]
        )
    else:
        headers[OSS_COPY_OBJECT_SOURCE] = (
            "/" + source_bucket_name + "/" + urlquote(source_key, "")
        )

    range_string = _make_range_string(byte_range)
    if range_string:
        headers[OSS_COPY_OBJECT_SOURCE_RANGE] = range_string

    logger.debug("Start to upload part copy")

    parameters["uploadId"] = target_upload_id
    parameters["partNumber"] = str(target_part_number)

    resp = await self._do_object("PUT", target_key, params=parameters, headers=headers)
    logger.debug("Upload part copy done")

    return PutObjectResult(resp)


async def list_parts(
    self: "AioBucket",
    key: str,
    upload_id: str,
    marker: str = "",
    max_parts: int = 1000,
    headers: Optional[Union[Dict, CaseInsensitiveDict]] = None,
) -> ListPartsResult:
    """list uploaded parts in a part uploading progress.

    Args:
        key (str): key value of the object.
        upload_id (str): the upload event id.
        marker (str, optional): paging separator.
        max_uploads (int, optional): max return numbers per page.
        headers (Optional[Union[Dict, CaseInsensitiveDict]], optional):
            HTTP headers to specify.

    Returns:
        ListPartsResult:
    """
    logger.debug("Start to list parts")

    headers = CaseInsensitiveDict(headers)

    resp = await self._do_object(
        "GET",
        key,
        params={
            "uploadId": upload_id,
            "part-number-marker": marker,
            "max-parts": str(max_parts),
        },
        headers=headers,
    )
    logger.debug("List parts done.")
    return await self._parse_result(resp, parse_list_parts, ListPartsResult)
