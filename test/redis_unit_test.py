import unittest
from unittest.mock import MagicMock, patch
import json

from database.redis_manager import RedisManager

class TestRedisManager(unittest.TestCase):

    @patch('database.redis_manager.redis.StrictRedis.from_url', autospec=True)
    def setUp(self, mock_from_url):
        # Create MockRedis instance
        self.mock_redis = MagicMock()
        mock_from_url.return_value = self.mock_redis
        
        # Initialize RedisManager
        self.redis_manager = RedisManager()

    def test_set_and_get(self):
        key = 'test_key'
        value = {'value': 123}

        self.redis_manager.set(key, value)
        self.mock_redis.set.assert_called_once_with(key, json.dumps(value), ex=None)

        # Mock check_bloom_filter to return True
        self.redis_manager.check_bloom_filter = MagicMock(return_value=True)
        
        self.mock_redis.get.return_value = json.dumps(value)
        result = self.redis_manager.get(key)
        self.mock_redis.get.assert_called_once_with(key)
        self.assertEqual(result, value)

    def test_get_non_existent_key(self):
        key = 'non_existent_key'

        self.redis_manager.check_bloom_filter = MagicMock(return_value=False)
        result = self.redis_manager.get(key)
        self.mock_redis.get.assert_not_called()
        self.assertIsNone(result)

    def test_delete(self):
        key = 'test_key'

        self.redis_manager.delete(key)
        self.mock_redis.delete.assert_called_once_with(key)

    def test_bloom_filter(self):
        key = 'test_key'
        self.redis_manager.add_to_bloom_filter(key)
        self.assertTrue(self.redis_manager.check_bloom_filter(key))

    def test_get_related_query_keys(self):
        record_id = 'record_id'
        expected_keys = {b'key1', b'key2'}
        self.mock_redis.smembers.return_value = expected_keys

        result = self.redis_manager.get_related_query_keys(record_id)
        self.mock_redis.smembers.assert_called_once_with(f'record_queries:{record_id}')
        self.assertEqual(result, expected_keys)

    def test_add_and_remove_related_query_key(self):
        record_id = 'record_id'
        query_key = 'query_key'

        self.redis_manager.add_related_query_key(record_id, query_key)
        self.mock_redis.sadd.assert_called_once_with(f'record_queries:{record_id}', query_key)

        self.redis_manager.remove_related_query_key(record_id, query_key)
        self.mock_redis.srem.assert_called_once_with(f'record_queries:{record_id}', query_key)

    @patch('database.redis_manager.Redlock.lock', autospec=True)
    @patch('database.redis_manager.Redlock.unlock', autospec=True)
    def test_lock_management(self, mock_unlock, mock_lock):
        # Mock the return value of the lock method
        mock_lock.return_value = MagicMock(validity=1000, resource='lock_key', key=b'lock_key')
        
        lock_key = 'lock_key'
        ttl = 1000

        # Acquire lock
        lock = self.redis_manager.acquire_lock(lock_key, ttl)
        print(f"lock: {lock}")  # Debug output
        mock_lock.assert_called_once_with(self.redis_manager.lock_manager, lock_key, ttl)
        self.assertIsNotNone(lock)

        # Release lock
        self.redis_manager.release_lock(lock)
        mock_unlock.assert_called_once_with(self.redis_manager.lock_manager, lock)

    def test_set_with_expiration(self):
        key = 'test_key'
        value = {'value': 123}
        ex = 60

        self.redis_manager.set(key, value, ex=ex)
        self.mock_redis.set.assert_called_once_with(key, json.dumps(value), ex=ex)

    def test_cache_null(self):
        key = 'test_key'
        ex = 60

        self.redis_manager.cache_null(key, ex=ex)
        self.mock_redis.set.assert_called_once_with(key, json.dumps(None), ex=ex)

    def test_exception_handling(self):
        key = 'test_key'
        value = {'value': 123}

        self.mock_redis.set.side_effect = Exception('Redis error')

        with self.assertRaises(Exception):
            self.redis_manager.set(key, value)

if __name__ == '__main__':
    unittest.main()
