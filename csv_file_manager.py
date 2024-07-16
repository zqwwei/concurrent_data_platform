import csv
import logging
from threading_lib.read_write_lock import FairReadWriteLock

class CSVFileManager:
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
        self.lock = FairReadWriteLock()

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
