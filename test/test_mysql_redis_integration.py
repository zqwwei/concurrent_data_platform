import unittest
import logging
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, Table, Column, String, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from database.mysql_manager import MySQLDatabase
import json

# 配置日志记录器
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestMySQLDatabaseRedisIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('mysql+pymysql://root:root@localhost:3306/testdb')
        cls.metadata = MetaData()
        cls.test_table = Table('record', cls.metadata,
                               Column('C1', String(255), primary_key=True),
                               Column('C2', String(255)),
                               Column('C3', String(255)))
        cls.metadata.create_all(cls.engine)
        cls.Session = scoped_session(sessionmaker(bind=cls.engine))

    @patch('database.redis_manager.redis.StrictRedis.from_url', autospec=True)
    def setUp(self, mock_redis_from_url):
        self.mock_redis = MagicMock()
        mock_redis_from_url.return_value = self.mock_redis
        self.db = MySQLDatabase('mysql+pymysql://root:root@localhost:3306/testdb', table_name='record')

        session = self.db.Session()
        session.execute(self.db.Record.__table__.delete())
        session.commit()
        session.close()

        self.mock_redis.flushall()

    def tearDown(self):
        self.db.Session.remove()

    def test_add_and_query_record_with_redis(self):
        logger.debug("Running test_add_and_query_record_with_redis")
        record = {'C1': '1', 'C2': 'test', 'C3': 'value1'}
        self.db.add_record([record['C1'], record['C2'], record['C3']])

        session = self.db.Session()
        result = session.query(self.db.Record).filter_by(C1=record['C1']).first()
        self.assertIsNotNone(result)
        self.assertEqual(result.C1, record['C1'])
        self.assertEqual(result.C2, record['C2'])
        self.assertEqual(result.C3, record['C3'])

        record_dict = {'C1': '1', 'C2': 'test', 'C3': 'value1'}
        self.mock_redis.set.assert_called_with(f'record:{record["C1"]}', json.dumps(record_dict), ex=None)

    def test_update_record_with_redis(self):
        logger.debug("Running test_update_record_with_redis")
        record = {'C1': '1', 'C2': 'test', 'C3': 'value1'}
        self.db.add_record([record['C1'], record['C2'], record['C3']])

        new_value = 'value2'
        self.db.update_record({'C1': record['C1']}, 'C3', new_value)

        session = self.db.Session()
        result = session.query(self.db.Record).filter_by(C1=record['C1']).first()
        self.assertIsNotNone(result)
        self.assertEqual(result.C3, new_value)

        updated_record = {'C1': record['C1'], 'C2': record['C2'], 'C3': new_value}
        self.mock_redis.set.assert_any_call(f'record:{record["C1"]}', json.dumps(updated_record), ex=None)

    def test_delete_record_with_redis(self):
        logger.debug("Running test_delete_record_with_redis")
        record = {'C1': '1', 'C2': 'test', 'C3': 'value1'}
        self.db.add_record([record['C1'], record['C2'], record['C3']])

        self.db.delete_record({'C1': record['C1']})

        session = self.db.Session()
        result = session.query(self.db.Record).filter_by(C1=record['C1']).first()
        self.assertIsNone(result)

        self.mock_redis.delete.assert_called_with(f'record:{record["C1"]}')

    def test_query_records_with_equality_condition(self):
        logger.debug("Running test_query_records_with_equality_condition")
        records = [
            {'C1': '1', 'C2': 'test', 'C3': 'value1'},
            {'C1': '2', 'C2': 'test2', 'C3': 'value2'},
            {'C1': '3', 'C2': 'test3', 'C3': 'value3'},
        ]
        for record in records:
            self.db.add_record([record['C1'], record['C2'], record['C3']])

        self.mock_redis.get.return_value = None

        conditions = [('C2', '==', 'test2', 'AND')]
        result = self.db.query_records(conditions)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['C2'], 'test2')

    def test_query_records_with_ilike_condition(self):
        logger.debug("Running test_query_records_with_ilike_condition")
        records = [
            {'C1': '1', 'C2': 'test', 'C3': 'value1'},
            {'C1': '2', 'C2': 'test2', 'C3': 'value2'},
            {'C1': '3', 'C2': 'test3', 'C3': 'value3'},
        ]
        for record in records:
            self.db.add_record([record['C1'], record['C2'], record['C3']])

        self.mock_redis.get.return_value = None

        conditions = [('C2', '$=', 'test', 'AND')]
        result = self.db.query_records(conditions)

        self.assertEqual(len(result), 3)

    def test_query_records_with_and_condition(self):
        logger.debug("Running test_query_records_with_and_condition")
        records = [
            {'C1': '1', 'C2': 'test', 'C3': 'value1'},
            {'C1': '2', 'C2': 'test2', 'C3': 'value2'},
            {'C1': '3', 'C2': 'test', 'C3': 'value3'},
        ]
        for record in records:
            self.db.add_record([record['C1'], record['C2'], record['C3']])

        self.mock_redis.get.return_value = None

        conditions = [('C2', '==', 'test', 'AND'), ('C3', '==', 'value1', 'AND')]
        result = self.db.query_records(conditions)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['C3'], 'value1')

    def test_query_records_with_or_condition(self):
        logger.debug("Running test_query_records_with_or_condition")
        records = [
            {'C1': '1', 'C2': 'test', 'C3': 'value1'},
            {'C1': '2', 'C2': 'test2', 'C3': 'value2'},
            {'C1': '3', 'C2': 'test', 'C3': 'value3'},
        ]
        for record in records:
            self.db.add_record([record['C1'], record['C2'], record['C3']])

        self.mock_redis.get.return_value = None

        conditions = [('C2', '==', 'test', 'OR'), ('C3', '==', 'value2', 'OR')]
        result = self.db.query_records(conditions)

        self.assertEqual(len(result), 3)

    @classmethod
    def tearDownClass(cls):
        cls.metadata.drop_all(cls.engine)
        cls.engine.dispose()


if __name__ == '__main__':
    unittest.main()
