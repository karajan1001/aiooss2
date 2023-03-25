"""
Module for resumable operations
"""
import asyncio
import logging
import os
from typing import TYPE_CHECKING, Callable, Collection, Dict, Mapping, Optional, Union

from oss2 import Bucket, CryptoBucket
from oss2.compat import to_string, to_unicode
from oss2.defaults import multiget_threshold as MULTIGET_THRESHOLD
from oss2.defaults import multipart_num_threads as MULTIPART_NUM_THREADS
from oss2.defaults import multipart_threshold as MULTIPART_THRESHOLD
from oss2.defaults import part_size as PART_SIZE
from oss2.exceptions import InconsistentError
from oss2.headers import (
    IF_MATCH,
    IF_UNMODIFIED_SINCE,
    OSS_OBJECT_ACL,
    OSS_REQUEST_PAYER,
    OSS_SERVER_SIDE_DATA_ENCRYPTION,
    OSS_SERVER_SIDE_ENCRYPTION,
    OSS_TRAFFIC_LIMIT,
)
from oss2.http import CaseInsensitiveDict
from oss2.models import ContentCryptoMaterial, MultipartUploadCryptoContext, PartInfo
from oss2.resumable import (
    ResumableStore,
    _filter_invalid_headers,
    _ObjectInfo,
    _PartToProcess,
    _populate_valid_headers,
    _populate_valid_params,
    _ResumableDownloader,
    _ResumableUploader,
    determine_part_size,
)
from oss2.utils import (
    b64decode_from_string,
    b64encode_as_string,
    calc_obj_crc_from_parts,
    check_crc,
    force_rename,
    http_date,
)

from aiooss2.adapter import FilelikeObjectAdapter
from aiooss2.exceptions import InvalidEncryptionRequest
from aiooss2.iterators import AioPartIterator
from aiooss2.utils import copyfileobj_and_verify

if TYPE_CHECKING:
    from oss2.models import PutObjectResult

    from aiooss2.api import AioBucket


logger = logging.getLogger(__name__)


async def resumable_upload(  # pylint: disable=too-many-arguments
    bucket: Union["AioBucket", "CryptoBucket"],
    key: Union[str, bytes],
    filename: Union[str, bytes],
    store: Optional["ResumableStore"] = None,
    headers: Optional[Mapping] = None,
    multipart_threshold: Optional[int] = None,
    part_size: Optional[int] = None,
    progress_callback: Optional[Callable] = None,
    num_threads: Optional[int] = None,
    params: Optional[Mapping] = None,
) -> "PutObjectResult":
    """Resumable upload local file , The implementation is splitting local
    files to multipart, storing uploading information in local files. If the
    uploading was interrupted by some reasons, only those remaied parts need
    to be uploaded.

    # Using `CryptoBucket` will make the upload fallback to the normal one.

    Args:
        bucket (Union[AioBucket, CryptoBucket]): bucket object to upload
        key (Union[str, bytes]): object key to store the file
        filename (Union[str, bytes]): filename to upload
        store (Optional["ResumableStore"]): ResumableStore object to keep the
            uploading info in the previous operation. Defaults to None.
        headers (Optional[Mapping]): HTTP headers to send. Defaults to None.
            # put_object or init_multipart_upload can make use of the whole
                headers
            # uplpad_part only accept OSS_REQUEST_PAYER, OSS_TRAFFIC_LIMIT
            # complete_multipart_upload only accept OSS_REQUEST_PAYER,
                OSS_OBJECT_ACL
        multipart_threshold (Optional[int]): threshold to use multipart upload
            instead of a normal one. Defaults to None.
        part_size (Optional[int]): partition size of the multipart.
            Defaults to None.
        progress_callback (Optional[Callable]): callback function for
            progress bar. Defaults to None.
        num_threads (Optional[int]): concurrency number during the uploading
            Defaults to None.
        params (Optional[Mapping]): Defaults to None.

    Returns:
        PutObjectResult:
    """
    key_str = to_string(key)
    filename_str = to_unicode(filename)
    size = os.path.getsize(filename_str)

    logger.debug(
        "Start to resumable upload, bucket: %s, key: %s, filename: %s, "
        "headers: %s, multipart_threshold: %s, part_size: %s, "
        "num_threads: %s, size of file to upload is %s",
        bucket.bucket_name,
        key_str,
        filename_str,
        headers,
        multipart_threshold,
        part_size,
        num_threads,
        size,
    )
    multipart_threshold = multipart_threshold or MULTIPART_THRESHOLD
    num_threads = num_threads or MULTIPART_NUM_THREADS
    part_size = part_size or PART_SIZE

    if size >= multipart_threshold and not isinstance(bucket, CryptoBucket):
        store = store or ResumableStore()
        uploader = ResumableUploader(
            bucket,
            key_str,
            filename_str,
            size,
            store,
            part_size=part_size,
            headers=headers,
            progress_callback=progress_callback,
            num_threads=num_threads,
            params=params,
        )
        result = await uploader.upload()
    else:
        result = await bucket.put_object_from_file(
            key_str,
            filename_str,
            headers=headers,
            progress_callback=progress_callback,
        )

    return result


async def resumable_download(  # pylint: disable=too-many-arguments
    bucket: Union["AioBucket", "CryptoBucket"],
    key: Union[str, bytes],
    filename: Union[str, bytes],
    store: Optional["ResumableStore"] = None,
    headers: Optional[Mapping] = None,
    part_size: Optional[int] = None,
    progress_callback: Optional[Callable] = None,
    num_threads: Optional[int] = None,
    params: Optional[Mapping] = None,
    multiget_threshold: Optional[int] = None,
) -> None:
    """Resumable download object to local file, implementation method is to
    create a list temporary files whose name is formed by the original
    filename with some random surfix. If the downloading was interrupted by
    some reasons, only those remaied parts need to be downloaded.

    # Using `CryptoBucket` will make the download fallback to the normal one.
    # Avoid using multi-threading/processing as the temp downloaded file might
    # be covered.

    Args:
        bucket (Union[AioBucket, CryptoBucket]): bucket object to download
        key (Union[str, bytes]): object key to store the file
        filename (Union[str, bytes]): filename to download
        store (Optional["ResumableStore"]): ResumableStore object to keep the
            downloading info in the previous operation.
        headers (Optional[Mapping]): HTTP headers to send. Defaults to None.
            # download_part only accept OSS_REQUEST_PAYER
            # get_object and get_object_to_file only accept OSS_REQUEST_PAYER,
                OSS_TRAFFIC_LIMIT
        multipget_threshold (Optional[int]): threshold to use multipart
            download instead of a normal one.
        part_size (Optional[int]): partition size of the multipart.
        progress_callback (Optional[Callable]): callback function for
            progress bar.
        num_threads (Optional[int]): concurrency number during the uploadinging
        params (Optional[Mapping]):

    Return:
        None:
    """
    key_str: str = to_string(key)
    filename_str: str = to_unicode(filename)

    logger.debug(
        "Start to resumable download, bucket: %s, key: %s, filename: %s, "
        "multiget_threshold: %s, part_size: %s, num_threads: %s",
        bucket.bucket_name,
        key_str,
        filename_str,
        multiget_threshold,
        part_size,
        num_threads,
    )
    multiget_threshold = multiget_threshold or MULTIGET_THRESHOLD

    valid_headers = _populate_valid_headers(
        headers, [OSS_REQUEST_PAYER, OSS_TRAFFIC_LIMIT]
    )
    result = await bucket.head_object(key_str, params=params, headers=valid_headers)
    logger.debug("The size of object to download is: %s", result.content_length)
    if result.content_length >= multiget_threshold:
        downloader = ResumableDownloader(
            bucket,
            key,
            filename,
            _ObjectInfo.make(result),
            part_size=part_size,
            progress_callback=progress_callback,
            num_threads=num_threads,
            store=store,
            params=params,
            headers=valid_headers,
        )
        await downloader.download(result.server_crc)
    else:
        await bucket.get_object_to_file(
            key_str,
            filename_str,
            progress_callback=progress_callback,
            params=params,
            headers=valid_headers,
        )


class ResumableUploader(_ResumableUploader):
    """Resumable Uploader"""

    bucket: "AioBucket"

    async def upload(  # pylint: disable=invalid-overridden-method
        self,
    ) -> "PutObjectResult":
        """resumable upload file to oss storage

        Returns:
            PutObjectResult:
        """
        await self._load_record()

        parts_to_upload: Collection["_PartToProcess"] = self.__get_parts_to_upload(
            self.__finished_parts
        )
        parts_to_upload = sorted(parts_to_upload, key=lambda p: p.part_number)
        logger.debug("Parts need to upload: %s", parts_to_upload)

        sem = asyncio.Semaphore(self.__num_threads)
        tasks = [
            asyncio.ensure_future(self._upload_task(sem, part_to_upload))
            for part_to_upload in parts_to_upload
        ]
        await asyncio.gather(*tasks)

        self._report_progress(self.size)

        headers = _populate_valid_headers(
            self.__headers, [OSS_REQUEST_PAYER, OSS_OBJECT_ACL]
        )
        result = await self.bucket.complete_multipart_upload(
            self.key, self.__upload_id, self.__finished_parts, headers=headers
        )
        self._del_record()

        return result

    async def _upload_task(
        self, sem: "asyncio.Semaphore", part_to_upload: _PartToProcess
    ):
        async with sem:
            return await self._upload_part(part_to_upload)

    async def _upload_part(self, part: _PartToProcess):
        with open(to_unicode(self.filename), "rb") as f_r:
            self._report_progress(self.__finished_size)

            f_r.seek(part.start, os.SEEK_SET)
            headers = _populate_valid_headers(
                self.__headers, [OSS_REQUEST_PAYER, OSS_TRAFFIC_LIMIT]
            )
            if self.__encryption:
                result = await self.bucket.upload_part(
                    self.key,
                    self.__upload_id,
                    part.part_number,
                    FilelikeObjectAdapter(f_r, size=part.size),
                    headers=headers,
                    upload_context=self.__upload_context,
                )
            else:
                result = await self.bucket.upload_part(
                    self.key,
                    self.__upload_id,
                    part.part_number,
                    FilelikeObjectAdapter(f_r, size=part.size),
                    headers=headers,
                )

            logger.debug(
                "Upload part success, add part info to record, part_number: "
                "%s, etag: %s, size: %s",
                part.part_number,
                result.etag,
                part.size,
            )
            self.__finish_part(
                PartInfo(
                    part.part_number,
                    result.etag,
                    size=part.size,
                    part_crc=result.crc,
                )
            )

    def _verify_record(self, record: Optional[Dict]):
        if record and not self.__is_record_sane(record):
            logger.warning("The content of record is invalid, delete the record")
            self._del_record()
            return None

        if record and self.__file_changed(record):
            logger.warning(
                "File: %s has been changed, delete the record", self.filename
            )
            self._del_record()
            return None

        if record and not self.__upload_exists(record["upload_id"]):
            logger.warning(
                "Multipart upload: %s does not exist, delete the record",
                record["upload_id"],
            )
            self._del_record()
            return None
        return record

    async def init_record(self) -> Dict:
        """Initialization record for the file to upload."""
        params = _populate_valid_params(self.__params, [Bucket.SEQUENTIAL])
        part_size = determine_part_size(self.size, self.__part_size)
        logger.debug(
            "Upload File size: %d, User-specify part_size: %d, "
            "Calculated part_size: %d",
            self.size,
            self.__part_size,
            part_size,
        )
        if self.__encryption:
            upload_context = MultipartUploadCryptoContext(self.size, part_size)
            init_result = await self.bucket.init_multipart_upload(
                self.key, self.__headers, params, upload_context
            )
            upload_id = init_result.upload_id
            if self.__record_upload_context:
                material = upload_context.content_crypto_material
                material_record = {
                    "wrap_alg": material.wrap_alg,
                    "cek_alg": material.cek_alg,
                    "encrypted_key": b64encode_as_string(material.encrypted_key),
                    "encrypted_iv": b64encode_as_string(material.encrypted_iv),
                    "mat_desc": material.mat_desc,
                }
        else:
            init_result = await self.bucket.init_multipart_upload(
                self.key, self.__headers, params
            )
            upload_id = init_result.upload_id

        record = {
            "op_type": self.__op,
            "upload_id": upload_id,
            "file_path": self._abspath,
            "size": self.size,
            "mtime": self.__mtime,
            "bucket": self.bucket.bucket_name,
            "key": self.key,
            "part_size": part_size,
        }

        if self.__record_upload_context:
            record["content_crypto_material"] = material_record

        logger.debug(
            "Add new record, bucket: %s, key: %s, upload_id: %s, part_size: %d",
            self.bucket.bucket_name,
            self.key,
            upload_id,
            part_size,
        )

        self._put_record(record)
        return record

    async def _get_finished_parts(self):
        parts = []

        valid_headers = _filter_invalid_headers(
            self.__headers,
            [OSS_SERVER_SIDE_ENCRYPTION, OSS_SERVER_SIDE_DATA_ENCRYPTION],
        )

        async for part in AioPartIterator(
            self.bucket, self.key, self.__upload_id, headers=valid_headers
        ):
            parts.append(part)

        return parts

    async def _load_record(self):
        record: Optional[Dict] = self._get_record()
        logger.debug("Load record return %s", record)

        record: Optional[Dict] = self._verify_record(record)

        record: Dict = record or await self.init_record()

        self.__record: Dict = record
        self.__part_size: int = self.__record["part_size"]
        self.__upload_id = self.__record["upload_id"]
        if self.__record_upload_context:
            if "content_crypto_material" in self.__record:
                material_record = self.__record["content_crypto_material"]
                wrap_alg = material_record["wrap_alg"]
                cek_alg = material_record["cek_alg"]
                if (
                    cek_alg != self.bucket.crypto_provider.cipher.alg
                    or wrap_alg != self.bucket.crypto_provider.wrap_alg
                ):
                    err_msg = (
                        "Envelope or data encryption/decryption "
                        "algorithm is inconsistent"
                    )
                    raise InconsistentError(err_msg, self)
                content_crypto_material = ContentCryptoMaterial(
                    self.bucket.crypto_provider.cipher,
                    material_record["wrap_alg"],
                    b64decode_from_string(material_record["encrypted_key"]),
                    b64decode_from_string(material_record["encrypted_iv"]),
                    material_record["mat_desc"],
                )
                self.__upload_context = MultipartUploadCryptoContext(
                    self.size, self.__part_size, content_crypto_material
                )

            else:
                err_msg = (
                    "If record_upload_context flag is true, "
                    "content_crypto_material must in the the record"
                )
                raise InconsistentError(err_msg, self)

        else:
            if "content_crypto_material" in self.__record:
                err_msg = (
                    "content_crypto_material must in the the record, "
                    "but record_upload_context flat is false"
                )
                raise InvalidEncryptionRequest(err_msg, self)

        self.__finished_parts = await self._get_finished_parts()
        self.__finished_size = sum(p.size for p in self.__finished_parts)


# pylint: disable=too-few-public-methods
class ResumableDownloader(_ResumableDownloader):
    """Resumable Downloader"""

    bucket: "AioBucket"

    async def download(  # pylint: disable=invalid-overridden-method
        self, server_crc=None
    ):
        """Resumable download object from oss server to a local file.

        Args:
            server_crc (_type_, optional): _description_. Defaults to None.
        """
        self.__load_record()

        parts_to_download = self.__get_parts_to_download()
        logger.debug("Parts need to download: %s", parts_to_download)

        with open(self.__tmp_file, "ab"):
            pass

        sem = asyncio.Semaphore(self.__num_threads)
        tasks = [
            asyncio.ensure_future(self._download_task(sem, part_to_download))
            for part_to_download in parts_to_download
        ]
        await asyncio.gather(*tasks)

        if self.bucket.enable_crc:
            parts = sorted(self.__finished_parts, key=lambda p: p.part_number)
            object_crc = calc_obj_crc_from_parts(parts)
            check_crc("resume download", object_crc, server_crc, None)

        force_rename(self.__tmp_file, self.filename)

        self._report_progress(self.size)
        self._del_record()

    async def _download_task(
        self, sem: "asyncio.Semaphore", part_to_upload: _PartToProcess
    ):
        async with sem:
            return await self._download_part(part_to_upload)

    async def _download_part(self, part: _PartToProcess):
        self._report_progress(self.__finished_size)

        with open(self.__tmp_file, "rb+") as f_r:
            f_r.seek(part.start, os.SEEK_SET)

            headers = _populate_valid_headers(
                self.__headers, [OSS_REQUEST_PAYER, OSS_TRAFFIC_LIMIT]
            )
            if headers is None:
                headers = CaseInsensitiveDict()
            headers[IF_MATCH] = self.objectInfo.etag
            headers[IF_UNMODIFIED_SINCE] = http_date(self.objectInfo.mtime)

            result = await self.bucket.get_object(
                self.key,
                byte_range=(part.start, part.end - 1),
                headers=headers,
                params=self.__params,
            )
            await copyfileobj_and_verify(
                result,
                f_r,
                part.end - part.start,
                request_id=result.request_id,
            )

        part.part_crc = result.client_crc
        logger.debug(
            "down part success, add part info to record, part_number:"
            " %s, start: %s, end: %s",
            part.part_number,
            part.start,
            part.end,
        )

        self.__finish_part(part)
