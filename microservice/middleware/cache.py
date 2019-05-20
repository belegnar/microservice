import json

import aioredis
from aioredis.util import _converters

from microservice.middleware.objects import BasicObject


class RedisCache:
    def __init__(self, redis_connection: aioredis.Redis):
        self.redis = redis_connection

    async def store_request(self, request_hash, expire, value):
        data = json.dumps(value)
        await self.redis.setex("request.{}".format(request_hash), expire, data)

    async def restore_answer(self, request_hash):
        data = await self.redis.get("request.{}".format(request_hash))
        answer = json.loads(data or json.dumps(data))
        return answer

    async def check_request(self, request_hash):
        return await self.redis.exists("request.{}".format(request_hash))

    async def set(self, key, expire=0, value=None):
        """set value as json"""
        data = json.dumps(value)
        await self.redis.setex(key, expire, data)

    async def get(self, key):
        """restore value as json"""
        data = await self.redis.get(key)
        if data is not None:
            value = json.loads(data)
        else:
            value = None
        return value

    async def store(self, key, expire=0, o=None):
        """
        Store BasicObject to redis (jsoned)
        :param key:
        :param expire:
        :param o: BasicObject
        :return: None
        """
        if o:
            data = o.json()
        else:
            data = None
        if expire:
            await self.redis.setex(key, expire, data)
        else:
            await self.redis.set(key, data)

    async def restore(self, key, object_class=BasicObject):
        """
        Restore object from redis
        :param key:
        :param object_class:
        :return: object_class()
        """
        value = await self.redis.get(key)
        if value:
            o = object_class.from_json(value)
        else:
            o = None
        return o

    async def exists(self, key):
        return await self.redis.exists(key)

    async def list(self, prefix):
        keys = await self.redis.keys("{}*".format(prefix))
        items = []
        if keys:
            items = [BasicObject.from_json(x) if x is not None else x for x in await self.redis.mget(*keys)]
        return items

    async def delete(self, key):
        await self.redis.delete(key)

    async def sadd(self, key, value, *values):
        """
        Add one or more members to a set
        :param key:
        :param value:
        :return: number of added elements
        """
        if isinstance(value, tuple(_converters.keys())):
            def dumper(x): return x
        else:
            dumper = json.dumps
        return await self.redis.sadd(key, dumper(value), *[dumper(v) for v in values])

    async def spop(self, key):
        """
        Remove and return a random member from a set
        :param key:
        :return: any
        """
        item = await self.redis.spop(key, encoding="utf-8")
        return json.loads(item) if item is not None else item

    async def sstore(self, key, o=None):
        """
        Store BasicObject to redis set (json'ed)
        :param key:
        :param o: BasicObject
        :return: number of added elements
        """
        if o:
            data = o.json()
        else:
            data = json.dumps(o)
        return await self.sadd(key, data)

    async def srestore(self, key, object_class=BasicObject):
        """
        Remove and return `object_class` from redis set
        :param key:
        :param object_class:
        :return: object_class()
        """
        value = await self.spop(key)
        if value:
            o = object_class(item_dict=value)
        else:
            o = None
        return o

    def close(self):
        self.redis.close()

    async def wait_closed(self):
        await self.redis.wait_closed()
