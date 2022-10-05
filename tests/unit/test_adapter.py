"""Unit tests for contents in adapter.py"""
# pylint: disable=too-many-arguments
# pylint: disable=missing-function-docstring
import asyncio
import inspect
import io
import os
from typing import TYPE_CHECKING

import pytest
from _collections_abc import list_iterator
from aiohttp.streams import StreamReader
from mock import mock

from aiooss2.adapter import (
    FilelikeObjectAdapter,
    IterableAdapter,
    SliceableAdapter,
)
from aiooss2.utils import make_adapter

if TYPE_CHECKING:
    from oss2 import Bucket

    from aiooss2.api import AioBucket


@pytest.fixture(scope="module", name="test_path")
def file_level_path(test_directory):
    file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"{test_directory}/{file_name}"


def _async_iterator():
    async def async_range(count):
        for i in range(count):
            yield str(i).encode("utf-8")

    return async_range(10)


def _string_io():
    return io.StringIO("string file object")


def _bytes_io():
    return io.BytesIO(b"byte file object: \x00\x01")


def _buffer_reader():
    return io.BufferedReader(io.BytesIO(b"string file object"))


def _iterator():
    return iter(["iter", "able", "string"])


def _mock_stream():
    """Mock a stream with data."""
    protocol = mock.Mock(_reading_paused=False)
    stream = StreamReader(protocol, limit=2**16)
    stream.feed_data(b"data file object")
    stream.feed_eof()
    return stream


@pytest.mark.parametrize(
    "datastream,adapter_obj,data_get",
    [
        ("string", SliceableAdapter, b"string"),
        (b"bytes", SliceableAdapter, b"bytes"),
        (bytearray(b"bytearray"), SliceableAdapter, b"bytearray"),
        (
            _string_io,
            FilelikeObjectAdapter,
            b"string file object",
        ),
        (
            _bytes_io,
            FilelikeObjectAdapter,
            b"byte file object: \x00\x01",
        ),
        (
            _buffer_reader,
            FilelikeObjectAdapter,
            b"string file object",
        ),
        (
            _mock_stream,
            FilelikeObjectAdapter,
            b"data file object",
        ),
        (
            _iterator,
            IterableAdapter,
            b"iterablestring",
        ),
        (_async_iterator, IterableAdapter, b"0123456789"),
    ],
)
@pytest.mark.parametrize("enable_crc", [True, False])
def test_make_adapter(
    bucket: "AioBucket",
    oss2_bucket: "Bucket",
    test_path: str,
    datastream,
    adapter_obj,
    data_get: bytes,
    enable_crc: bool,
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if callable(datastream):
        datastream = datastream()

    if not enable_crc and isinstance(datastream, list_iterator):
        pytest.skip("invalid combination")

    function_name = inspect.stack()[0][0].f_code.co_name
    adapter = make_adapter(datastream, enable_crc=enable_crc)
    object_name = f"{test_path}/{function_name}"
    if enable_crc:
        assert isinstance(adapter, adapter_obj)

    async def put(object_name, data):
        async with bucket as aiobucket:
            return await aiobucket.put_object(object_name, data)

    bucket.enable_crc = enable_crc
    result = asyncio.run(put(object_name, datastream))
    bucket.enable_crc = not enable_crc
    assert result.resp.status == 200

    assert oss2_bucket.get_object(object_name).read() == data_get


def test_make_adapter_error():
    with pytest.raises(ValueError):
        assert make_adapter(
            ["data1", "data2", "data3"], discard=1, enable_crc=True
        )
