from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor
from csv_file_manager import CSVFileManager
from data_filter import DataFilter
from data_modifier import DataModifier
from threading_lib.read_write_lock import FairReadWriteLock
from threading_lib.task_queue import LocalQueue
import threading 
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)


class CSVDatabase:
    """
    A class that represents a CSV database, allowing for querying and modifying the data.
    
    Attributes:
        file_manager: An instance of CSVFileManager to manage CSV file reading and writing.
        data_modifier: An instance of DataModifier to handle data modification commands.
        query_parser: An instance of QueryParser to parse query strings.
        data_filter: An instance of DataFilter to filter data based on queries.
    """

    def __init__(self, filepath, max_workers=10):
        """
        Initializes the CSVDatabase with the given CSV file path.

        :param filepath: Path to the CSV file.
        """
        # store file, and manage read and write
        self.file_manager = CSVFileManager(filepath)
        # parse command, and process (insert, update, delete)
        self.data_modifier = DataModifier(self.file_manager)
        # process check data: 
        self.data_filter = DataFilter(self.file_manager)
        self.lock = FairReadWriteLock()
        self.task_queue = LocalQueue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._start_queue_consumer()

    def _start_queue_consumer(self):
        def consumer():
            while True:
                command = self.task_queue.get()
                print(command)
                if command is None:
                    break
                self._process_write_command(command)
                self.task_queue.task_done()
        consumer_thread = threading.Thread(target=consumer)
        consumer_thread.daemon = True
        consumer_thread.start()

    def _process_write_command(self, command):
        with self.lock.write_lock():
            self.data_modifier.parse_command(command)
            if self.file_manager.data_modified:
                self.file_manager.write()  

    def query_data(self, query_str):
        """
        Parses the query string and filters the data accordingly.

        :param query_str: A SQL-like query string.
        :return: Filtered data based on the query.
        """
        with self.lock.read_lock():
            results = self.data_filter.filter(query_str)
            return results

    def modify_data(self, command):
        """
        Parses the modification command and applies it to the data.

        :param command: A SQL-like command for data modification.
        """
        self.task_queue.put(command)

    def stop_consumer(self):
        self.task_queue.put(None)



# Initliaze CSVDatabase
csv_database = CSVDatabase('./test/test_data.csv')

# Route to handle queries
@app.route('/', methods=['GET'])
def handle_query_request():
    """
    Handles incoming requests for querying or modifying the CSV data.
    Expects either a 'query' or 'job' parameter in the URL.

    :return: JSON response with the result of the query or modification.
    """
    query = request.args.get('query')
    if query:
        logger.debug(f"Received query: {query}")
        future = csv_database.executor.submit(csv_database.query_data, query)
        results = future.result()
        logger.debug(f"Query results: {results}")
        return jsonify({'result': results})
    else:
        logger.debug("No valid parameters provided")
        return jsonify({'msg': 'No valid parameters provided'}), 400

# Route to handle data modification
@app.route('/', methods=['POST'])
def handle_modify_request():
    data = request.get_json()
    job = data.get('job')
    if job:
        logger.debug(f"Received job: {job}")
        csv_database.modify_data(job)
        return jsonify({'result': 'Success'})
    else:
        logger.debug("No valid job parameter provided")
        return jsonify({'msg': 'No valid job parameter provided'}), 400

# Main function to start the server
if __name__ == '__main__':
    app.run(debug=True, port=5000)