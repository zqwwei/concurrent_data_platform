import csv
import logging
from database.database_interface import DatabaseInterface

class CSVFileManager(DatabaseInterface):
    def __init__(self, filepath):
        """
        Initializes the CSVFileManager to read data from the specified filepath
        and write changes to the file at specified intervals if data has been modified.

        :param filepath: Path to the CSV file.
        :param write_interval: Interval in seconds between checks for writing modified data to file.
        """
        self.filepath = filepath
        self.data = self.read()
        self.data_modified = False

    def read(self):
        """
        Loads CSV data from the file specified by self.filepath. Data is loaded into a list of dictionaries,
        each representing a row with column names as keys.

        :return: List of dictionaries containing the CSV data, or None if an error occurs.
        """
        data = []
        try:
            with open(self.filepath, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                data = list(reader)
        except FileNotFoundError:
            logging.error(f"File not found: {self.filepath}")
            return None
        except csv.Error as e:
            logging.error(f"CSV format error in file: {self.filepath}, error: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            return None
        return data

    def write(self):
        """
        Writes the current in-memory data to the CSV file specified by self.filepath.
        This method is thread-safe and ensures that data is only written if it has been modified.
        """
        try:
            with open(self.filepath, 'w', newline='') as csvfile:
                if self.data:
                    writer = csv.DictWriter(csvfile, fieldnames=self.data[0].keys())
                    writer.writeheader()
                    writer.writerows(self.data)
                    self.data_modified = False
        except Exception as e:
            logging.error(f"Failed to write data to {self.filepath}: {e}")

    def add_record(self, record):
        new_row = dict(zip(self.data[0].keys(), record))
        self.data.append(new_row)
        self.data_modified = True

    def delete_record(self, conditions):
        original_row_len = len(self.data)
        self.data = [row for row in self.data if not all(row.get(k) == v for k, v in conditions.items())]
        if original_row_len > len(self.data):
            self.data_modified = True

    def update_record(self, conditions, target_column, new_value):
        for row in self.data:
            if all(row.get(k) == v for k, v in conditions.items()):
                row[target_column] = new_value
                self.data_modified = True

    def query_records(self, query_conditions):
        filtered_data = []
        for row in self.data:
            match = False
            last_logic = 'and'
            for i, condition in enumerate(query_conditions):
                column, operator, value, logic = condition
                condition_match = self.check_condition(row, (column, operator, value))
                if last_logic == 'and':
                    match = condition_match if i == 0 else match and condition_match
                elif last_logic == 'or':
                    match = match or condition_match
                if logic == '':
                    break
                last_logic = logic
            if match:
                filtered_data.append(row)
        return filtered_data

    def check_condition(self, row, condition):
        column, operator, value = condition
        if column == '*':
            return self.check_all_columns(row, operator, value)
        else:
            cell_value = row.get(column, "")
            return self.evaluate_condition(cell_value, operator, value)

    def check_all_columns(self, row, operator, value):
        for cell_value in row.values():
            if not self.evaluate_condition(cell_value, operator, value):
                return False
        return True

    def evaluate_condition(self, cell_value, operator, value):
        if operator == '==':
            return cell_value == value
        elif operator == '!=':
            return cell_value != value
        elif operator == '$=':
            return cell_value.lower() == value.lower()
        elif operator == '&=':
            return value in cell_value
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def get_columns(self):
        return list(self.data[0].keys()) if self.data else []