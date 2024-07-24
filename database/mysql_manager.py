from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from database.database_interface import DatabaseInterface
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
        logging.debug(f"Initialized MySQLDatabase with table: {table_name}, columns: {self.column_names}")

    def dynamic_table_class(self, table_name):
        table = self.metadata.tables.get(table_name)
        if table is None:
            logging.error(f"Table '{table_name}' not found in the database.")
            return None
        
        # 获取表的主键列
        primary_key_columns = [col.name for col in table.primary_key.columns]

        if not primary_key_columns:
            raise ValueError(f"Table '{table_name}' does not have a primary key.")
        
        logging.debug(f"Primary key columns for table '{table_name}': {primary_key_columns}")

        # 创建一个动态的 ORM 类
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
            deleted_count = query.delete()
            session.commit()
            logging.debug("Deleted {deleted_count} records successfully")
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
            query.update({getattr(self.Record, target_column): new_value})
            session.commit()
            logging.debug(f"Updated {updated} records")
        except Exception as e:
            logging.error(f"Error updating records: {e}")
            session.rollback()
        finally:
            session.close()

    def query_records(self, query_conditions):
        logging.debug(f"Querying records with conditions: {query_conditions}")
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
            return [record.to_dict() for record in result]
        except Exception as e:
            logging.error(f"Error querying records: {e}")
            session.close()
        finally:
            session.close()
    
    def get_columns(self):
        return self.column_names