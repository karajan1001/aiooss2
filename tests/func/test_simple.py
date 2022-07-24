"""
Basic tests for aiooss2
"""
# pylint: disable=missing-function-docstring
import asyncio
import inspect
import os
from typing import TYPE_CHECKING

import pytest
from oss2 import ObjectIterator

from aiooss2 import AioObjectIterator

if TYPE_CHECKING:

    from oss2 import Bucket

    from aiooss2 import AioBucket


@pytest.fixture(scope="module", name="test_path")
def file_level_path(test_directory):
    file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"{test_directory}/{file_name}"


def test_put_object(bucket: "AioBucket", oss2_bucket: "Bucket", test_path):
    data = b"\x01" * 1024
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"

    async def put(object_name, data):
        async with bucket as aiobucket:
            return await aiobucket.put_object(object_name, data)

    result = asyncio.run(put(object_name, data))
    assert result.resp.status == 200
    assert oss2_bucket.get_object(object_name).read() == data


def test_get_object(bucket: "AioBucket", oss2_bucket: "Bucket", test_path):
    data = b"\x01" * 1024
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"
    oss2_bucket.put_object(object_name, data)

    async def get(object_name):
        async with bucket as aiobucket:
            result = await aiobucket.get_object(object_name)
            return await result.read()

    result = asyncio.run(get(object_name))
    assert result == data


def test_list_objects(bucket: "AioBucket", oss2_bucket: "Bucket", test_path):
    data = b"\x01" * 2
    function_name = inspect.stack()[0][0].f_code.co_name
    object_path = f"{test_path}/{function_name}"
    expected = []
    for i in range(10):
        obj = f"{object_path}/{i}"
        oss2_bucket.put_object(obj, data)
        expected.append(obj)

    async def list_obj(bucket):
        async with bucket as aiobucket:
            result = []
            async for obj in AioObjectIterator(aiobucket, prefix=object_path):
                result.append(obj.key)
            return result

    result = asyncio.run(list_obj(bucket))
    assert result == expected


def test_delete_object(bucket: "AioBucket", oss2_bucket: "Bucket", test_path):
    data = b"\x01" * 1024
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"

    oss2_bucket.put_object(object_name, data)
    file_list = [
        obj.key for obj in ObjectIterator(oss2_bucket, prefix=object_name)
    ]
    assert object_name in file_list

    async def delete_obj(object_name):
        async with bucket as aiobucket:
            return await aiobucket.delete_object(object_name)

    asyncio.run(delete_obj(object_name))

    file_list = [
        obj.key for obj in ObjectIterator(oss2_bucket, prefix=object_name)
    ]
    assert object_name not in file_list
