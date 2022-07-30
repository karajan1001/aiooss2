"""
Pytest setup
"""
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
import os
import pathlib
import subprocess
import time
import uuid

import pytest
import requests
from oss2 import Bucket, Service

from aiooss2 import AioBucket, AioService, Auth

PORT = 5555
LICENSE_PATH = os.path.join(
    pathlib.Path(__file__).parent.parent.resolve(), "LICENSE"
)
NUMBERS = b"1234567890\n"


@pytest.fixture(scope="session")
def access_key_id():
    return os.environ.get("OSS_ACCESS_KEY_ID", "")


@pytest.fixture(scope="session")
def access_key_secret():
    return os.environ.get("OSS_SECRET_ACCESS_KEY", "")


@pytest.fixture(scope="session")
def endpoint():
    return os.environ.get("OSS_ENDPOINT")


@pytest.fixture(scope="session")
def bucket_name():
    return os.environ.get("OSS_TEST_BUCKET_NAME")


@pytest.fixture(scope="session")
def test_directory():
    test_id = uuid.uuid4()
    return f"ossfs_test/{test_id}"


@pytest.fixture(scope="session")
def test_full_path(bucket_name, test_directory):
    return f"/{bucket_name}/{test_directory}"


@pytest.fixture(scope="session")
def emulator_endpoint():
    return f"http://127.0.0.1:{PORT}/"


@pytest.fixture()
def oss_emulator_server_start(emulator_endpoint):
    """
    Start a local emulator server
    """
    with subprocess.Popen(f"ruby bin/emulator -r store -p {PORT}"):

        timeout = 5
        while timeout > 0:
            try:
                result = requests.get(emulator_endpoint)
                if result.ok:
                    break
            except Exception as err:
                raise Exception("emulator start timeout") from err
            timeout -= 0.1
            time.sleep(0.1)
        yield


@pytest.fixture(scope="session")
def auth(access_key_id, access_key_secret):
    return Auth(access_key_id, access_key_secret)


@pytest.fixture()
def oss2_bucket(auth, endpoint, bucket_name):
    return Bucket(auth, endpoint, bucket_name)


@pytest.fixture()
def bucket(auth, endpoint, bucket_name):
    return AioBucket(auth, endpoint, bucket_name)


@pytest.fixture()
def oss2_service(auth, endpoint):
    return Service(auth, endpoint)


@pytest.fixture()
def service(auth, endpoint):
    return AioService(auth, endpoint)


@pytest.fixture(scope="session")
def file_in_anonymous(auth, endpoint, test_directory):
    anonymous_bucket = "dvc-anonymous"
    filename = f"{test_directory}/file"
    bucket = Bucket(auth, endpoint, anonymous_bucket)
    bucket.put_object(filename, "foobar")
    return f"/{bucket}/{filename}"


@pytest.fixture(scope="session")
def number_file(auth, test_bucket_name, endpoint, test_directory):
    filename = f"{test_directory}/number"
    bucket = Bucket(auth, endpoint, test_bucket_name)
    bucket.put_object(filename, NUMBERS)
    return f"/{test_bucket_name}/{filename}"


@pytest.fixture(scope="session")
def license_file(auth, test_bucket_name, endpoint, test_directory):
    filename = f"{test_directory}/LICENSE"
    bucket = Bucket(auth, endpoint, test_bucket_name)
    bucket.put_object_from_file(filename, LICENSE_PATH)
    return f"/{test_bucket_name}/{filename}"
