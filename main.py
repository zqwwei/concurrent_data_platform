from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from concurrent.futures import ThreadPoolExecutor
from database.csv_manager import CSVFileManager
from database.mysql_manager import MySQLDatabase
from business_logic import BusinessLogic
from threading_lib.read_write_lock import FairReadWriteLock
from threading_lib.task_queue import LocalQueue, RabbitMQQueue
import threading 
import logging
import time
import config
import pymysql

pymysql.install_as_MySQLdb()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object('config')
db = SQLAlchemy(app)


class CSVDatabase:
    """
    A class that represents a CSV database, allowing for querying and modifying the data.
    
    Attributes:
        file_manager: An instance of CSVFileManager to manage CSV file reading and writing.
        data_modifier: An instance of DataModifier to handle data modification commands.
        query_parser: An instance of QueryParser to parse query strings.
        data_filter: An instance of DataFilter to filter data based on queries.
    """

    def __init__(self, db_type, db_url, max_workers=10, batch_size=10, delay=5, use_rabbitmq=False):
        """
        Initializes the CSVDatabase with the given CSV file path.

        :param filepath: Path to the CSV file.
        """
        # # store file, and manage read and write
        # self.file_manager = CSVFileManager(filepath)
        # # parse command, and process (insert, update, delete)
        # self.data_modifier = DataModifier(self.file_manager)
        # process check data: 
        # self.data_filter = DataFilter(self.file_manager)
        self.lock = FairReadWriteLock()
        self.task_queue = RabbitMQQueue() if use_rabbitmq else LocalQueue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.batch_size = batch_size
        self.delay = delay
        self.db_type = db_type
        self.db_url = db_url
        self._init_db()
        self._start_batch_consumer()
        self._init_business_logic()

    def _init_db(self):
        if self.db_type == 'csv':
            self.db = CSVFileManager(self.db_url)
        elif self.db_type == 'mysql':
            self.db = MySQLDatabase(self.db_url)
        else:
            raise ValueError("Unsupported database type")
    
    def _init_business_logic(self):
        BusinessLogic.initialize(self.db)

    def _start_batch_consumer(self):
        def batch_processor():
            while True:
                time.sleep(self.delay)
                commands = self.task_queue.get(self.batch_size)
                if commands:
                    self._process_write_commands(commands)
                    for _ in commands:
                        self.task_queue.task_done()

        consumer_thread = threading.Thread(target=batch_processor)
        consumer_thread.daemon = True
        consumer_thread.start()

    def _process_write_commands(self, commands):
        with self.lock.write_lock():
            for command in commands:
                if command is None:
                    break
                # self.data_modifier.parse_command(command)
            # if self.file_manager.data_modified:
            #     self.file_manager.write()  
                BusinessLogic.modify_data(command)
                self.db.write()

    def query_data(self, query_str):
        """
        Parses the query string and filters the data accordingly.

        :param query_str: A SQL-like query string.
        :return: Filtered data based on the query.
        """
        with self.lock.read_lock():
            return BusinessLogic.query_data(query_str)
             

    def modify_data(self, command):
        """
        Parses the modification command and applies it to the data.

        :param command: A SQL-like command for data modification.
        """
        self.task_queue.put(command)

    def stop_consumer(self):
        self.task_queue.close()


# Initliaze CSVDatabase
@app.route('/init', methods=['POST'])
def initialize_database():
    data = request.get_json()
    db_type = data.get('db_type')
    db_url = data.get('db_url')
    use_rabbitmq = data.get('use_rabbitmq', False)
    max_workers = data.get('max_workers', 10)

    if not db_type or not db_url:
        return jsonify({'msg': 'db_type and db_url are required'}), 400
    
    global csv_database
    try:
        csv_database = CSVDatabase(db_type, db_url, max_workers=max_workers, use_rabbitmq=use_rabbitmq)
        return jsonify({'result': 'Database initialized successfully'})
    except ValueError as e:
        return jsonify({'msg': str(e)}), 400

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