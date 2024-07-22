from sqlalchemy import create_engine, Column, Integer, String, and_, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from database.database_interface import DatabaseInterface

Base = declarative_base()

class MySQLDatabase(DatabaseInterface):
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def read(self):
        session = self.Session()
        result = session.query(Record).all()
        session.close()
        return [record.to_dict() for record in result]
    
    def write(self):
        pass

    def add_record(self, record):
        session = self.Session()
        new_record = Record(**record)
        session.add(new_record)
        session.commit()
        session.close()
    
    def delete_record(self, conditions):
        session = self.Session()
        query = session.query(Record)
        for key, value in conditions.items():
            query = query.filter(getattr(Record, key) == value)
        query.delete()
        session.commit()
        session.close()
    
    def update_record(self, conditions, target_column, new_value):
        session = self.Session()
        query = session.query(Record)
        for key, value in conditions.items():
            query = query.filter(getattr(Record, key) == value)
        query.update({getattr(Record, target_column): new_value})
        session.commit()
        session.close()

    def query_records(self, query_conditions):
        session = self.Session()
        query = session.query(Record)
        conditions_list = []
        for condition in query_conditions:
            column, operator, value, logic = condition
            if operator == '==':
                cond = getattr(Record, column) == value
            elif operator == '!=':
                cond = getattr(Record, column) != value
            elif operator == '$=':
                cond = getattr(Record, column).ilike(f'%{value}%')
            elif operator == '&=':
                cond = getattr(Record, column).contains(value)
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
        session.close()
        return [record.to_dict() for record in result]
    

class Record(Base):
    __tablename__ = 'record'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    def to_dict(self):
        return {'id':self.id, 'name':self.name}