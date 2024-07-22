from database.query_parser import QueryParser
from database.data_filter import DataFilter
from database.database_interface import DatabaseInterface

class business_logic:
    def __init__(self, db):
        self.db = db
        self.query_parser = QueryParser()
        self.data_modifier = DataModifier(self.db)

    def query_data(self, command):
        return self.db.query_records(command)
    
    def modify_data(self, command):
        self.data_modifier.parse_command(command)