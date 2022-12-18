"""
Functional tests for resumable operations.
"""
# pylint: disable=missing-function-docstring
# pylint: disable=too-many-arguments
import asyncio
import inspect
import os
from typing import TYPE_CHECKING

import pytest

from aiooss2.resumable import resumable_upload

if TYPE_CHECKING:
    from oss2 import Bucket
    from py.path import local

    from aiooss2 import AioBucket


@pytest.fixture(scope="module", name="test_path")
def file_path(test_directory):
    test_file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"{test_directory}/{test_file_name}"


@pytest.mark.parametrize("enable_crc", [True, False])
@pytest.mark.parametrize("size", [100, 2**20])
def test_resumable_upload_file(
    tmpdir: "local",
    bucket: "AioBucket",
    oss2_bucket: "Bucket",
    test_path,
    enable_crc: bool,
    size: int,
):
    data = os.urandom(size)
    file = tmpdir / "file"
    file_w = tmpdir / "file_result"
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"
    file.write(data, mode="wb")

    async def resumalbe_upload_coroutine(object_name, file):
        async with bucket as aiobucket:
            return await resumable_upload(
                aiobucket,
                object_name,
                file,
                multipart_threshold=10,
                part_size=100 * 2**10,
            )

    bucket.enable_crc = enable_crc
    result = asyncio.run(resumalbe_upload_coroutine(object_name, str(file)))
    assert result.resp.status == 200
    oss2_bucket.get_object_to_file(object_name, file_w)
    assert oss2_bucket.get_object(object_name).read() == data
