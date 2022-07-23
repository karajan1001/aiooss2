"""
Contains some useful iteraotrs, can be used to iterate buckets、
files or file parts etc.
"""

from oss2 import ObjectIterator
from oss2.models import ListObjectsResult, SimplifiedObjectInfo

from .api import AioBucket
from .exceptions import ServerError


# pylint: disable=too-few-public-methods
class AioObjectIterator(ObjectIterator):
    """Iterator to iterate objects from a bucket.
    Return `SimplifiedObjectInfo <oss2.models.SimplifiedObjectInfo>`
    object. if `SimplifiedObjectInfo.is_prefix()` is true, the object
    returned is a directory.

    :param bucket: bucket class to iterate
    :param prefix: prefix to filter the object results
    :param delimiter: delimiter in object name
    :param marker: paginator
    :param max_keys: key number returns from `list_objects` every time,
        to notice that iterator can return more objects than it
    :param max_retries: retry number
    :param headers: HTTP header
    """

    bucket: AioBucket

    async def _fetch(self):  # pylint: disable=invalid-overridden-method

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

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self.entries:
                return self.entries.pop(0)

            if not self.is_truncated:
                raise StopAsyncIteration

            await self.fetch_with_retry()

    async def fetch_with_retry(
        self,
    ):  # pylint: disable=invalid-overridden-method
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
