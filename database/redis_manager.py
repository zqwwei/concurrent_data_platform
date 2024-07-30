import redis
import json
from pybloom_live import ScalableBloomFilter

class RedisManager:
    def __init__(self, redis_url='redis://localhost:6379/0'):
        self.client = redis.StrictRedis.from_url(redis_url)
        self.bloom_filter = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)

    def add_to_bloom_filter(self, key):
        self.bloom_filter.add(key)

    def check_bloom_filter(self, key):
        return key in self.bloom_filter

    def get(self, key):
        if not self.check_bloom_filter(key):
            return None
        value = self.client.get(key)
        return json.loads(value) if value else None

    def set(self, key, value, ex=None):
        self.client.set(key, json.dumps(value), ex=ex)

    def delete(self, key):
        self.client.delete(key)

    def get_related_query_keys(self, record_id):
        return self.client.smembers(f'record_queries:{record_id}')
    
    def add_related_query_key(self, record_id, query_key):
        self.client.sadd(f'record_queries:{record_id}', query_key)

    def remove_related_query_key(self, record_id, query_key):
        self.client.srem(f'record_queries:{record_id}', query_key)

    def get_query_result(self, query_key):
        value = self.client.get(query_key)
        return json.loads(value) if value else None

    def set_query_result(self, query_key, record_ids, ex=None):
        self.client.set(query_key, json.dumps(record_ids), ex=ex)

    def cache_null(self, key, ex=60):
        self.client.set(key, json.dumps(None), ex=ex)

