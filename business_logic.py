from database.query_parser import QueryParser
from database.data_modifier import DataModifier

class BusinessLogic:
    db = None
    query_parser = QueryParser()
    data_modifier = None

    @classmethod
    def initialize(cls, db):
        cls.db = db
        cls.data_modifier = DataModifier(cls.db)
    
    @classmethod
    def query_data(cls, command):
        return cls.db.query_records(cls.query_parser.parse_command(command))
    
    @classmethod
    def modify_data(cls, command):
        cls.data_modifier.parse_command(command)
