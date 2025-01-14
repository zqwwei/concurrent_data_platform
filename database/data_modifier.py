import re
from urllib.parse import unquote
import logging

logger = logging.getLogger(__name__)

class DataModifier:
    """
    A class to modify data of a CSV file based on SQL-like commands. Supports INSERT, DELETE, and UPDATE operations.
    """

    def __init__(self, db):
        """
        Initializes a DataModifier instance with a reference to a CSVFileManager.

        :param file_manager: The CSVFileManager instance whose data will be modified.
        """
        self.db = db
        self.columns = self.db.get_columns()

    def parse_command(self, command):
        """
        Parses a command string to determine and invoke the corresponding data modification method.

        :param command: A SQL-like command string (INSERT, DELETE, or UPDATE).
        :raises ValueError: If the command is not recognized.
        """
        command = unquote(command)
        if command.startswith("INSERT"):
            self.parse_insert(command[len("INSERT"):].strip())  
        elif command.startswith("DELETE"):
            self.parse_delete(command[len("DELETE"):].strip())  
        elif command.startswith("UPDATE"):
            self.parse_update(command[len("UPDATE"):].strip())
        else:
            raise ValueError("Unknown command")
    

    def parse_insert(self, command):
        """
        Parses an INSERT command and adds the specified values to the in-memory data.

        :param command: A SQL-like INSERT command string.
        :raises ValueError: If the command format is invalid or the column count does not match.
        """
        # print(f"Command: {command}")
        pattern = r'((?:"(?:[^"\\]|\\.)*"\s*,\s*)*(?:"(?:[^"\\]|\\.)*"))'
        match = re.match(pattern, command)
        if not match:
            raise ValueError("Invalid INSERT command format.")
        
        values = re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(1))
        expected_columns_count = len(self.columns)
        if len(values) != expected_columns_count:
            raise ValueError(f"Column count mismatch. Expected {expected_columns_count}, got {len(values)}.")
        
        # process escape characters
        processed_values = [value.replace('\\"', '"').replace('\\\\', '\\') for value in values]
        
        self.db.add_record(processed_values)


    def parse_delete(self, command):
        """
        Parses a DELETE command and removes the specified rows from the in-memory data.

        :param command: A SQL-like DELETE command string.
        :raises ValueError: If the command format is invalid or does not match the expected column count.
        """
        pattern = r'"((?:[^"\\]|\\.)*)"'
        values = re.findall(pattern, command)
        
        if len(values) < 1:
            raise ValueError("Too few conditions for DELETE command. Check conditions are surrounded by \"\".")
        
        expected_columns_count = len(self.columns)
        if len(values) > expected_columns_count:
            raise ValueError(f"Too many conditions for DELETE command. CSV file has {expected_columns_count} columns.")
        
        processed_values = [value.replace('\\"', '"').replace('\\\\', '\\') for value in values]

        conditions_dict = {self.columns[i]: processed_values[i] for i in range(len(processed_values))}

        self.db.delete_record(conditions_dict)

    def parse_update(self, command):
        """
        Parses an UPDATE command and applies the specified changes to the in-memory data.

        :param command: A SQL-like UPDATE command string.
        :raises ValueError: If the command format is invalid, conditions are not in pairs, or the target column does not exist.
        """
        pattern = r'(?:"((?:[^"\\]|\\.)*)"|\b([A-Za-z0-9_]+)\b)'
        parts = re.findall(pattern, command)

        flattened_parts = [quoted if quoted else unquoted for quoted, unquoted in parts]
        
        # print(flattened_parts)
        if len(flattened_parts) < 3:
            raise ValueError("UPDATE command must include at least one condition, target column, and a new value.")

        condition_parts = flattened_parts[:-2]
        target_column = flattened_parts[-2]
        new_value = flattened_parts[-1]

        condition_parts = [part.replace('\\"', '"').replace('\\\\', '\\') for part in condition_parts]
        new_value = new_value.replace('\\"', '"').replace('\\\\', '\\')

        if target_column not in self.columns:
            raise ValueError(f"Target column '{target_column}' does not exist in the CSV file.")

        conditions_dict = {self.columns[i]: condition_parts[i] for i in range(min(len(condition_parts), len(self.columns)))}
        self.db.update_record(conditions_dict, target_column, new_value)