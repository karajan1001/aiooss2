"""
Basic tests for bucket operations.
"""
# pylint: disable=missing-function-docstring
import asyncio
from typing import TYPE_CHECKING

from oss2 import BucketIterator

from aiooss2 import AioBucketIterator

if TYPE_CHECKING:

    from oss2 import Service

    from aiooss2 import AioService


def test_list_bucket(
    service: "AioService", oss2_service: "Service", bucket_name
):
    async def list_bucket():
        async with service as aioservice:
            result = []
            async for obj in AioBucketIterator(aioservice):
                result.append(obj.name)
            return result

    result = asyncio.run(list_bucket())
    assert bucket_name in result

    expected = []
    for obj in BucketIterator(oss2_service):
        expected.append(obj.name)
    assert expected == result
