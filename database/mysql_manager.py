from sqlalchemy import create_engine, MetaData, Table, Column, String, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import and_, or_
from sqlalchemy.orm import sessionmaker, scoped_session
from database.database_interface import DatabaseInterface
from database.redis_manager import RedisManager
import time
import logging
import traceback

Base = declarative_base()

class MySQLDatabase(DatabaseInterface):
    def __init__(self, db_url, table_name='record'):
        self.engine = create_engine(db_url)
        self.metadata = MetaData()

        # Create table if not exists
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            self.create_table(table_name)

        self.metadata.reflect(self.engine)
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        self.Record = self.dynamic_table_class(table_name)
        self.column_names = [column.name for column in self.Record.__table__.columns]
        self.redis = RedisManager()
        logging.debug(f"Initialized MySQLDatabase with table: {table_name}, columns: {self.column_names}")

    def create_table(self, table_name):
        table = Table(table_name, self.metadata,
                      Column('C1', String(255), primary_key=True),
                      Column('C2', String(255)),
                      Column('C3', String(255))
                      )
        self.metadata.create_all(self.engine)
        logging.info(f"Table '{table_name}' created successfully.")

    def dynamic_table_class(self, table_name):
        table = self.metadata.tables.get(table_name)
        if table is None:
            logging.error(f"Table '{table_name}' not found in the database.")
            return None

        primary_key_columns = [col.name for col in table.primary_key.columns]
        if not primary_key_columns:
            raise ValueError(f"Table '{table_name}' does not have a primary key.")

        logging.debug(f"Primary key columns for table '{table_name}': {primary_key_columns}")

        # Create ORM class with unique name
        class_name = f"DynamicRecord_{table_name}_{int(time.time())}"

        class DynamicRecord(Base):
            __tablename__ = table_name
            __table__ = table
            __mapper_args__ = {'primary_key': [table.c[pk] for pk in primary_key_columns]}

            def to_dict(self):
                return {column.name: getattr(self, column.name) for column in self.__table__.columns}

        return DynamicRecord


        
    def read(self):
        logging.debug("Reading all records")
        session = self.Session()
        try:
            result = session.query(self.Record).all()
            logging.debug(f"Read {len(result)} records")
            return [record.__dict__ for record in result]
        finally:
            session.close()
    
    def write(self):
        pass

    def add_record(self, record):
        logging.debug(f"Adding record: {record}")
        session = self.Session()
        try:
            new_record = self.Record(**dict(zip(self.column_names, record)))
            session.add(new_record)
            session.commit()
            logging.debug("Record added successfully")

            # Update Redis cache
            record_key = f'record:{new_record.C1}'
            record_dict = new_record.to_dict()
            logging.debug(f"Setting Redis key: {record_key} with value: {record_dict}")
            logging.debug(f"Record dictionary: {record_dict} of type {type(record_dict)}")
            self.redis.set(record_key, record_dict)
            self.redis.add_to_bloom_filter(record_key)
            self._invalidate_related_query_cache(new_record.C1)
        except Exception as e:
            logging.error(f"Error adding record: {e}")
            logging.error(traceback.format_exc())
            session.rollback()
        finally:
            session.close()

    def update_record(self, conditions, target_column, new_value):
        logging.debug(f"Updating records with conditions: {conditions}, setting {target_column} to {new_value}")
        session = self.Session()
        try:
            query = session.query(self.Record)
            for key, value in conditions.items():
                query = query.filter(getattr(self.Record, key) == value)
            records = query.all()
            updated_count = query.update({getattr(self.Record, target_column): new_value})
            session.commit()
            logging.debug(f"Updated {updated_count} records")

            # Update Redis cache
            for record in records:
                updated_record = session.query(self.Record).filter(self.Record.C1 == record.C1).first()
                record_dict = updated_record.to_dict()
                logging.debug(f"Setting Redis key: record:{updated_record.C1} with value: {record_dict}")
                self.redis.set(f'record:{updated_record.C1}', record_dict)
                self._invalidate_related_query_cache(updated_record.C1)
        except Exception as e:
            logging.error(f"Error updating records: {e}")
            logging.error(traceback.format_exc())
            session.rollback()
        finally:
            session.close()
    
    def delete_record(self, conditions):
        logging.debug(f"Deleting records with conditions: {conditions}")
        session = self.Session()
        try:
            query = session.query(self.Record)
            for key, value in conditions.items():
                query = query.filter(getattr(self.Record, key) == value)
            records = query.all()
            deleted_count = query.delete()
            session.commit()
            logging.debug(f"Deleted {deleted_count} records successfully")

            # Update Redis cache
            for record in records:
                logging.debug(f"Deleting Redis key: record:{record.C1}")
                self.redis.delete(f'record:{record.C1}')
                self._invalidate_related_query_cache(record.C1)
        except Exception as e:
            logging.error(f"Error deleting records: {e}")
            session.rollback()
        finally:
            session.close()

    def query_records(self, query_conditions):
        logging.debug(f"Querying records with conditions: {query_conditions}")
        query_key = f"query:{query_conditions}"
        cached_result = self.redis.get_query_result(query_key)
        if cached_result is not None:
            logging.debug("Cache hit for query")
            results = []
            for record_id in cached_result:
                record_key = f"record:{record_id}"
                if self.redis.check_bloom_filter(record_key) is None:
                    return []
                record = self.redis.get(record_key)
                if record:
                    results.append(record)
                else:
                    lock = self.redis.acquire_lock(record_key)
                    if lock:
                        try:
                            record = self._query_database_by_id(record_id)
                            if record:
                                result.append(record)
                                self.redis.set(record_id, record.to_dict(), ex=3600)
                            else:
                                self.redis.cache_null(record_key)
                        finally:
                            self.redis.release_lock(lock)
                    else:
                        # If lock is not acquired, retry after a short delay
                        time.sleep(0.1)
                        record = self.redis.get(record_key)
                        if record:
                            results.append(record)
            return results
        else:
            lock = self.redis.acquire_lock(query_key)
            if lock:
                try:
                    # double check if query was cached by another thread
                    cached_result = self.redis.get_query_result(query_key)
                    if cached_result is not None:
                        return [self.redis.get(f"record:{record_id}") for record_id in cached_result]
                    session = self.Session()
                    try:
                        query = session.query(self.Record)
                        conditions_list = []
                        for condition in query_conditions:
                            column, operator, value, logic = condition
                            if operator == '==':
                                cond = getattr(self.Record, column) == value
                            elif operator == '!=':
                                cond = getattr(self.Record, column) != value
                            elif operator == '$=':
                                cond = getattr(self.Record, column).ilike(f'%{value}%')
                            elif operator == '&=':
                                cond = getattr(self.Record, column).contains(value)
                            else:
                                raise ValueError(f"Unsupported operator: {operator}")
                            
                            conditions_list.append((cond, logic))

                        # Combine conditions with AND/OR logic
                        combined_conditions = []
                        current_conditions = []

                        for cond, logic in conditions_list:
                            current_conditions.append(cond)
                            if logic.lower() == 'or':
                                combined_conditions.append(and_(*current_conditions))
                                current_conditions = []
                        
                        if current_conditions:
                            combined_conditions.append(and_(*current_conditions))
                        
                        if combined_conditions:
                            final_condition = or_(*combined_conditions)
                            query = query.filter(final_condition)

                        result = query.all()
                        logging.debug(f"Queried {len(result)} records")
                        record_ids = [record.C1 for record in result]
                        self.redis.set_query_result(query_key, record_ids, ex=3600)
                        for record in result:
                            record_key = f"record:{record.C1}"
                            self.redis.set(record_key, record.to_dict(), ex=3600)
                            self.redis.add_related_query_key(record.C1, query_key)
                        return [record.to_dict() for record in result]
                    except Exception as e:
                        logging.error(f"Error querying records: {e}")
                        session.close()
                    finally:
                        session.close()
                finally:
                    self.redis.release_lock(lock)
            else:
                # If lock is not acquired, retry after a short delay
                time.sleep(0.1)
                return self.query_records(query_conditions)
    
    def _query_database_by_id(self, record_id):
        session = self.Session()
        try:
            record = session.query(self.Record).filter(self.Record.C1 == record_id).first()
            return record.to_dict()
        finally:
            session.close()

    def _invalidate_related_query_cache(self, record_id):
        related_query_keys = self.redis.get_related_query_keys(record_id)
        for query_key in related_query_keys:
            logging.debug(f"Deleting related Redis query key: {query_key}")
            self.redis.delete(query_key)
            self.redis.remove_related_query_key(record_id, query_key)

    def get_columns(self):
        return self.column_names