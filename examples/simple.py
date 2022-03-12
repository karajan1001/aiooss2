import asyncio
from aiooss2 import Auth, Bucket, ObjectIterator

OSS_ACCESS_KEY_ID = "xxx"
OSS_SECRET_ACCESS_KEY = "xxx"


async def go():
    bucket = 'aiooss2-test'
    filename = 'your_file'
    newfile = 'new_file'
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