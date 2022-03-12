# aiooss2

Async client for aliyun OSS(Object Storage Service) using oss2 and aiohttp_/asyncio_.

The main purpose of this library is to support aliyun OSS api, but other services
should work (but maybe with minor fixes). For now, we have tested
only upload/download api for OSS. More functionality will be coming soon.

# Install

```bash
pip install aiooss2
```

## Basic Example
-------------

```python
import asyncio
from aiooss2 import Auth, Bucket, ObjectIterator

OSS_ACCESS_KEY_ID = "xxx"
OSS_SECRET_ACCESS_KEY = "xxx"


async def go():
    bucket = 'aiooss2-test'
    filename = 'your_file'
    folder = 'readme'
    file_obj = f'{folder}/{filename}'
    data_obj = f'{folder}/{data}'


    auth = Auth(OSS_ACCESS_KEY_ID , OSS_SECRET_ACCESS_KEY)
    bucket = Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', bucket)

    # upload object to oss
    data = b'\x01'*1024
    resp = await bucket.put_object(data, data_obj)
    print(resp)

    # upload object to oss
    obj_read = await bucket.get_obj(data, data_obj)
    assert obj_read == data

    # list oss objects
    async for b in ObjectIterator(bucket):
        print(b.key)

    # delete object 
    resp = await bucket.delete_object(file_obj)
    print(resp)
    async for b in ObjectIterator(bucket):
        print(b.key)

loop = asyncio.get_event_loop()
loop.run_until_complete(go())
```

## Run Tests
------------

Make sure you have development requirements installed and your oss key and secret accessible via environment variables:

```bash
$pip3 install -e "."
$export OSS_ACCESS_KEY_ID=xxx
$export OSS_SECRET_ACCESS_KEY=xxx
```

Execute tests suite:

```bash

    # upload file to oss
    resp = await bucket.put_object_from_file(file_obj, filename)
    print(resp)

    # get object to file
    obj_read = await bucket.get_object_to_file(file_obj, newfile)
    # this will ensure the connection is correctly re-used/closed
    with open('your_file') as f:
        assert await obj_read == data

    # list oss objects
    async for b in aiooss2.ObjectIterator(bucket):
        print(b.key)

    # delete object 
    resp = await bucket.delete_object()
    print(resp)

    # list s3 objects using paginator
    async for b in aiooss2.ObjectIterator(bucket):
        print(b.key)

loop = asyncio.get_event_loop()
loop.run_until_complete(go())
```

## Run Tests
------------

Make sure you have development requirements installed and your oss key and secret accessible via environment variables:

```bash
$pip3 install -e "."
$export OSS_ACCESS_KEY_ID=xxx
$export OSS_SECRET_ACCESS_KEY=xxx
```

Execute tests suite:

```bash
$pytest tests
```
