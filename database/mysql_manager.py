from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from database.database_interface import DatabaseInterface
from database.redis_manager import RedisManager
import logging

Base = declarative_base()

class MySQLDatabase(DatabaseInterface):
    def __init__(self, db_url, table_name='record'):
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.Record = self.dynamic_table_class(table_name)
        self.column_names = [column.name for column in self.Record.__table__.columns]
        self.redis = RedisManager()
        logging.debug(f"Initialized MySQLDatabase with table: {table_name}, columns: {self.column_names}")

    def dynamic_table_class(self, table_name):
        table = self.metadata.tables.get(table_name)
        if table is None:
            logging.error(f"Table '{table_name}' not found in the database.")
            return None
        
        primary_key_columns = [col.name for col in table.primary_key.columns]
        if not primary_key_columns:
            raise ValueError(f"Table '{table_name}' does not have a primary key.")
        
        logging.debug(f"Primary key columns for table '{table_name}': {primary_key_columns}")

        # create ORM class
        class DynamicRecord(Base):
            __table__ = table
            __mapper_args__ = {
                'primary_key': [table.c[pk] for pk in primary_key_columns]
            }

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
            # update Redis cache
            self.redis.set(f'record:{new_record.C1}', new_record)
            self.redis.add_to_bloom_filter(f'record:{new_record.C1}')
        except Exception as e:
            logging.error(f"Error adding record: {e}")
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
            logging.debug("Deleted {deleted_count} records successfully")
            # update Redis cache
            for record in records:
                self.redis.delete(f'record:{record.C1}')
                self._invalidate_query_cache(f'record:{record.C1}')
        except Exception as e:
            logging.error(f"Error deleting records: {e}")
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
            # upate Redis cache
            for record in records:
                updated_record = session.query(self.Record).filter(self.Record.C1 == record.C1).first()
                self.redis.set(f'record:{updated_record.C1}', updated_record)
                self._invalidate_query_cache(f'record:{updated_record.C1}')
        except Exception as e:
            logging.error(f"Error updating records: {e}")
            session.rollback()
        finally:
            session.close()

    def query_records(self, query_conditions):
        logging.debug(f"Querying records with conditions: {query_conditions}")
        query_key = f"query:{query_conditions}"
        cached_result = self.redis.get(query_key)
        if cached_result:
            logging.debug("Cache hit for query")
            results = []
            for record_id in cached_result:
                record_key = f"record:{record_id}"
                record = self.redis.get(record_key)
                if record:
                    results.append(record)
                else:
                    record = self._query_database_by_id(record_id)
                    if record:
                        result.append(record)
                        self.redis.set(record_id, record, ex=3600)
            return results
        else:
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
                self.redis.set(query_key, record_ids, ex=3600)
                for record in result:
                    self.redis.set(f"record:{record.C1}", record.to_dict(), ex=3600)
                return [record.to_dict() for record in result]
            except Exception as e:
                logging.error(f"Error querying records: {e}")
                session.close()
            finally:
                session.close()
    
    def _query_database_by_id(self, record_id):
        session = self.Session()
        try:
            record = session.query(self.Record).filter(self.Record.C1 == record_id).first()
            return record.to_dict()
        finally:
            session.close()
    
    
    def _invalidate_query_cache(self, record_id_key):
        self.redis.delete(record_id_key)
    
    def get_columns(self):
        return self.column_names