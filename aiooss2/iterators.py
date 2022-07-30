"""
Contains some useful iteraotrs, can be used to iterate buckets、
files or file parts etc.
"""

from typing import Dict, Optional

from oss2 import defaults, http
from oss2.models import ListObjectsResult, SimplifiedObjectInfo

from .api import AioBucket
from .exceptions import ServerError


class _AioBaseIterator:
    def __init__(self, marker: str, max_retries: Optional[str]):
        self.is_truncated = True
        self.next_marker = marker

        max_retries = defaults.get(max_retries, defaults.request_retries)
        self.max_retries = max_retries if max_retries > 0 else 1

        self.entries = []

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

    def __init__(  # pylint: disable=too-many-arguments
        self,
        bucket: AioBucket,
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
        self.entries = result.object_list + [
            SimplifiedObjectInfo(prefix, None, None, None, None, None)
            for prefix in result.prefix_list
        ]
        self.entries.sort(key=lambda obj: obj.key)
        return result.is_truncated, result.next_marker
