"""
Contains some useful iteraotrs, can be used to iterate buckets、
files or file parts etc.
"""
# pylint: disable=too-many-arguments
from typing import TYPE_CHECKING, Dict, List, Optional

from oss2 import defaults, http
from oss2.models import ListObjectsResult, SimplifiedObjectInfo

from .exceptions import ServerError

if TYPE_CHECKING:
    from oss2.models import SimplifiedBucketInfo
    from oss2.resumable import PartInfo

    from .api import AioBucket, AioService


class _AioBaseIterator:
    def __init__(self, marker: str, max_retries: Optional[int]):
        self.is_truncated = True
        self.next_marker = marker

        max_retries = max_retries or defaults.request_retries
        self.max_retries = max_retries if max_retries > 0 else 1

        self.entries: List = []

    async def _fetch(self):
        raise NotImplementedError

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self.entries:
                return self.entries.pop(0)

            if not self.is_truncated:
                raise StopAsyncIteration

            await self.fetch_with_retry()

    async def next(self):
        """Return the next element"""
        return await next(self)

    async def fetch_with_retry(self):
        """Retry and fetch a batch of elements"""
        for i in range(self.max_retries):
            try:
                self.is_truncated, self.next_marker = await self._fetch()
            except ServerError as err:
                if err.status // 100 != 5:
                    raise

                if i == self.max_retries - 1:
                    raise
            else:
                return


class AioObjectIterator(_AioBaseIterator):
    """Iterator to iterate objects from a bucket.
    Return `SimplifiedObjectInfo <oss2.models.SimplifiedObjectInfo>`
    object. if `SimplifiedObjectInfo.is_prefix()` is true, the object
    returned is a directory.
    """

    def __init__(
        self,
        bucket: "AioBucket",
        prefix: str = "",
        delimiter: str = "",
        marker: str = "",
        max_keys: int = 100,
        max_retries: Optional[int] = None,
        headers: Optional[Dict] = None,
    ):
        """_summary_

        Args:
            bucket (AioBucket): bucket class to iterate
            prefix (str, optional): prefix to filter the object results
            delimiter (str, optional): delimiter in object name
            marker (str, optional): paginate separator
            max_keys (int, optional): key number returns from `list_objects`
            every time,to notice that iterator can return more objects than it
            max_retries (Optional[int], optional): retry number
            headers (Optional[Dict], optional): HTTP header
        """
        super().__init__(marker, max_retries)

        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.max_keys = max_keys
        self.headers = http.CaseInsensitiveDict(headers)

    async def _fetch(self):
        result: ListObjectsResult = await self.bucket.list_objects(
            prefix=self.prefix,
            delimiter=self.delimiter,
            marker=self.next_marker,
            max_keys=self.max_keys,
            headers=self.headers,
        )
        self.entries: List["SimplifiedObjectInfo"] = result.object_list + [
            SimplifiedObjectInfo(prefix, None, None, None, None, None)
            for prefix in result.prefix_list
        ]
        self.entries.sort(key=lambda obj: obj.key)
        return result.is_truncated, result.next_marker


class AioBucketIterator(_AioBaseIterator):
    """Iterate over buckets of an user
    Return `SimplifiedBucketInfo <oss2.models.SimplifiedBucketInfo>` every
        iteration
    """

    def __init__(
        self,
        service: "AioService",
        prefix: str = "",
        marker: str = "",
        max_keys: int = 100,
        max_retries: Optional[int] = None,
    ):
        """

        Args:
            service (AioService): Service class of a special user.
            prefix (str, optional): prefix to filter the buckets results.
            marker (str, optional): paginate separator.
            max_keys (int, optional): max return number per page.
            max_retries (Optional[int], optional): max retry count.
        """
        super().__init__(marker, max_retries)
        self.service = service
        self.prefix = prefix
        self.max_keys = max_keys

    async def _fetch(self):
        result = await self.service.list_buckets(
            prefix=self.prefix, marker=self.next_marker, max_keys=self.max_keys
        )
        self.entries: List["SimplifiedBucketInfo"] = result.buckets

        return result.is_truncated, result.next_marker


class AioPartIterator(_AioBaseIterator):
    """Iterator over all parts from a partial upload session"""

    def __init__(
        self,
        bucket: "AioBucket",
        key: str,
        upload_id: str,
        marker: str = "0",
        max_parts: int = 1000,
        max_retries: Optional[int] = None,
        headers: Optional[Dict] = None,
    ):
        """

        Args:
            bucket (AioBucket): bucket to operate.
            key (str): key of the object.
            upload_id (str): upload id of the partial upload session.
            marker (str, optional): paginate separator.
            max_parts (int, optional): key number returns from `list_parts`
            max_retries (Optional[int], optional): max retry count.
            headers (Optional[Dict], optional): HTTP header.
        """
        super().__init__(marker, max_retries)

        self.bucket = bucket
        self.key = key
        self.upload_id = upload_id
        self.max_parts = max_parts
        self.headers = http.CaseInsensitiveDict(headers)

    async def _fetch(self):
        result = await self.bucket.list_parts(
            self.key,
            self.upload_id,
            marker=self.next_marker,
            max_parts=self.max_parts,
            headers=self.headers,
        )
        self.entries: List["PartInfo"] = result.parts

        return result.is_truncated, result.next_marker
