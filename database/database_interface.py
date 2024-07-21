from abc import ABC, abstractmethod

class DatabaseInterface(ABC):
    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def add_record(self, record):
        pass

    @abstractmethod
    def delete_record(self, conditions):
        pass


    @abstractmethod
    def update_record(self, conditions, target_column, new_value):
        pass

    @abstractmethod
    def query_records(self, query):
        pass
