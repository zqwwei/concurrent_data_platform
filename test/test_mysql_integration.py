import unittest
from sqlalchemy import create_engine, Table, Column, String, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session

class TestMySQLDatabaseIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup MySQL connection
        cls.engine = create_engine('mysql+pymysql://root:root@localhost:3306/testdb')
        cls.metadata = MetaData()
        cls.test_table = Table('record', cls.metadata,
                               Column('C1', String(255), primary_key=True),
                               Column('C2', String(255)),
                               Column('C3', String(255)))
        cls.metadata.create_all(cls.engine)
        cls.Session = scoped_session(sessionmaker(bind=cls.engine))

    def setUp(self):
        self.session = self.Session()
        # Clear the table before each test to avoid primary key conflicts
        self.session.execute(self.test_table.delete())
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_add_and_query_record(self):
        # Add a record
        ins = self.test_table.insert().values(C1='1', C2='test', C3='value1')
        self.session.execute(ins)
        self.session.commit()

        # Query the record
        sel = self.test_table.select().where(self.test_table.c.C1 == '1')
        result = self.session.execute(sel).fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], '1')  # Access by index
        self.assertEqual(result[1], 'test')
        self.assertEqual(result[2], 'value1')

    def test_update_record(self):
        # Add a record
        ins = self.test_table.insert().values(C1='1', C2='test', C3='value1')
        self.session.execute(ins)
        self.session.commit()

        # Update the record
        upd = self.test_table.update().where(self.test_table.c.C1 == '1').values(C3='value2')
        self.session.execute(upd)
        self.session.commit()

        # Query the updated record
        sel = self.test_table.select().where(self.test_table.c.C1 == '1')
        result = self.session.execute(sel).fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[2], 'value2')  # Access by index

    def test_delete_record(self):
        # Add a record
        ins = self.test_table.insert().values(C1='1', C2='test', C3='value1')
        self.session.execute(ins)
        self.session.commit()

        # Delete the record
        delete = self.test_table.delete().where(self.test_table.c.C1 == '1')
        self.session.execute(delete)
        self.session.commit()

        # Query the deleted record
        sel = self.test_table.select().where(self.test_table.c.C1 == '1')
        result = self.session.execute(sel).fetchone()

        self.assertIsNone(result)

    @classmethod
    def tearDownClass(cls):
        cls.metadata.drop_all(cls.engine)
        cls.engine.dispose()


if __name__ == '__main__':
    unittest.main()
