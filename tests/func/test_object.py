"""
Functional tests for object operations.
"""
# pylint: disable=missing-function-docstring
import asyncio
import inspect
import os
from typing import TYPE_CHECKING

import pytest
from oss2 import ObjectIterator

from aiooss2 import AioObjectIterator
from tests.conftest import NUMBERS

if TYPE_CHECKING:
    from oss2 import Bucket
    from py.path import local

    from aiooss2 import AioBucket


@pytest.fixture(scope="module", name="test_path")
def file_level_path(test_directory):
    file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"{test_directory}/{file_name}"


@pytest.mark.parametrize("enable_crc", [True, False])
def test_put_object(
    bucket: "AioBucket", oss2_bucket: "Bucket", test_path, enable_crc: bool
):
    data = b"\x01" * 1024
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"

    async def put(object_name, data):
        async with bucket as aiobucket:
            return await aiobucket.put_object(object_name, data)

    bucket.enable_crc = enable_crc
    result = asyncio.run(put(object_name, data))
    assert result.resp.status == 200
    assert oss2_bucket.get_object(object_name).read() == data


@pytest.mark.parametrize("enable_crc", [True, False])
def test_get_object(bucket: "AioBucket", number_file, enable_crc: bool):
    async def get(object_name):
        async with bucket as aiobucket:
            resp = await aiobucket.get_object(object_name)
            async with resp as result:
                return await result.read()

    bucket.enable_crc = enable_crc
    result = asyncio.run(get(number_file))
    assert result == NUMBERS


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
    file_list = [obj.key for obj in ObjectIterator(oss2_bucket, prefix=object_name)]
    assert object_name in file_list

    async def delete_obj(object_name):
        async with bucket as aiobucket:
            return await aiobucket.delete_object(object_name)

    asyncio.run(delete_obj(object_name))

    file_list = [obj.key for obj in ObjectIterator(oss2_bucket, prefix=object_name)]
    assert object_name not in file_list


def test_get_object_meta(bucket: "AioBucket", oss2_bucket: "Bucket", number_file):
    async def get_object_meta(object_name):
        async with bucket as aiobucket:
            return await aiobucket.get_object_meta(object_name)

    result = asyncio.run(get_object_meta(number_file))
    expected = oss2_bucket.get_object_meta(number_file)
    assert expected.content_length == result.content_length
    assert expected.last_modified == result.last_modified
    assert expected.etag == result.etag


def test_object_exists(bucket: "AioBucket", number_file):
    async def object_exists(object_name):
        async with bucket as aiobucket:
            return await aiobucket.object_exists(object_name)

    assert asyncio.run(object_exists(number_file))
    assert not asyncio.run(object_exists("non-exist"))


def test_get_bucket_info(bucket: "AioBucket", oss2_bucket: "Bucket"):
    async def get_bucket_info():
        async with bucket as aiobucket:
            return await aiobucket.get_bucket_info()

    result = asyncio.run(get_bucket_info())
    expected = oss2_bucket.get_bucket_info()
    assert result.name == expected.name
    assert result.location == expected.location
    assert result.creation_date == expected.creation_date
    assert result.comment == expected.comment
    assert result.owner.display_name == expected.owner.display_name
    assert result.owner.id == expected.owner.id
    assert result.acl.grant == expected.acl.grant


def test_append_object(bucket: "AioBucket", oss2_bucket: "Bucket", test_path):
    data = b"1234567890"
    append_data = b"abcdefg"
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"

    async def append_object(object_name, position, data):
        async with bucket as aiobucket:
            return await aiobucket.append_object(object_name, position, data)

    result = asyncio.run(append_object(object_name, 0, data))
    assert result.resp.status == 200
    assert oss2_bucket.get_object(object_name).read() == data

    result = asyncio.run(append_object(object_name, len(data), append_data))
    assert result.resp.status == 200
    assert oss2_bucket.get_object(object_name).read() == data + append_data


@pytest.mark.parametrize("enable_crc", [True, False])
def test_put_object_from_file(
    tmpdir: "local",
    bucket: "AioBucket",
    oss2_bucket: "Bucket",
    test_path,
    enable_crc: bool,
):
    data = b"123456789" * 10
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"
    file = tmpdir / "file"
    file.write(data)

    async def put_object_from_file(object_name, file):
        async with bucket as aiobucket:
            return await aiobucket.put_object_from_file(object_name, file)

    bucket.enable_crc = enable_crc
    result = asyncio.run(put_object_from_file(object_name, str(file)))
    assert result.resp.status == 200
    assert oss2_bucket.get_object(object_name).read() == data


@pytest.mark.parametrize("enable_crc", [True, False])
def test_get_object_to_file(
    tmpdir: "local", bucket: "AioBucket", number_file, enable_crc: bool
):
    file = tmpdir / "file"

    async def get_object_to_file(object_name, file):
        async with bucket as aiobucket:
            resp = await aiobucket.get_object_to_file(object_name, file)
            async with resp as result:
                return await result.read()

    bucket.enable_crc = enable_crc
    asyncio.run(get_object_to_file(number_file, str(file)))
    assert file.read_binary() == NUMBERS


def test_batch_delete_objects(bucket: "AioBucket", oss2_bucket: "Bucket", test_path):
    data = b"\x01" * 2
    function_name = inspect.stack()[0][0].f_code.co_name
    object_path = f"{test_path}/{function_name}"
    deleted = []
    expected = []
    for i in range(10):
        obj = f"{object_path}/{i}"
        oss2_bucket.put_object(obj, data)
        if i > 4:
            deleted.append(obj)
        else:
            expected.append(obj)

    async def batch_delete_objects(file_list):
        async with bucket as aiobucket:
            return await aiobucket.batch_delete_objects(file_list)

    result = asyncio.run(batch_delete_objects(deleted))
    assert [
        obj.key for obj in ObjectIterator(oss2_bucket, prefix=object_path)
    ] == expected
    assert result.deleted_keys == deleted


def test_copy_objects(
    bucket: "AioBucket",
    oss2_bucket: "Bucket",
    test_path,
    number_file,
    bucket_name,
):
    function_name = inspect.stack()[0][0].f_code.co_name
    object_path = f"{test_path}/{function_name}"

    async def copy_objects(file_from, file_to):
        async with bucket as aiobucket:
            return await aiobucket.copy_object(bucket_name, file_from, file_to)

    asyncio.run(copy_objects(number_file, object_path))
    assert [obj.key for obj in ObjectIterator(oss2_bucket, prefix=object_path)] == [
        object_path
    ]
    assert oss2_bucket.get_object(object_path).read() == NUMBERS


def test_put_object_from_middle_of_file(
    tmpdir: "local", bucket: "AioBucket", oss2_bucket: "Bucket", test_path
):
    data = b"1234567890\n"
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"
    file = tmpdir / "file"
    file.write(data * 2)

    async def put(object_name, data):
        async with bucket as aiobucket:
            return await aiobucket.put_object(object_name, data)

    with open(file, "rb") as f_r:
        assert f_r.readline() == data.strip() + os.linesep.encode()
        result = asyncio.run(put(object_name, f_r))
    assert result.resp.status == 200
    assert (
        oss2_bucket.get_object(object_name).read() == data.strip() + os.linesep.encode()
    )


def test_head_object(bucket: "AioBucket", oss2_bucket: "Bucket", number_file):
    async def head_object(object_name):
        async with bucket as aiobucket:
            return await aiobucket.head_object(object_name)

    result = asyncio.run(head_object(number_file))
    expected = oss2_bucket.head_object(number_file)
    assert expected.content_length == result.content_length
    assert expected.object_type == result.object_type
    assert expected.content_type == result.content_type
    assert expected.last_modified == result.last_modified
    assert expected.etag == result.etag
