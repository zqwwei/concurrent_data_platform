from flask import Flask, request, jsonify

from csv_file_manager import CSVFileManager
from query_parser import QueryParser
from data_filter import DataFilter
from data_modifier import DataModifier

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

    def __init__(self, filepath):
        """
        Initializes the CSVDatabase with the given CSV file path.

        :param filepath: Path to the CSV file.
        """
        # store file, and manage read and write
        self.file_manager = CSVFileManager(filepath)
        # parse command, and process (insert, update, delete)
        self.data_modifier = DataModifier(self.file_manager)
        self.query_parser = QueryParser()
        # process check data: 
        self.data_filter = DataFilter(self.file_manager)

    def query_data(self, query_str):
        """
        Parses the query string and filters the data accordingly.

        :param query_str: A SQL-like query string.
        :return: Filtered data based on the query.
        """
        conditions = self.query_parser.parse(query_str)
        # print(conditions)
        # print(self.file_manager.data)
        results = self.data_filter.filter(conditions)
        return results

    def modify_data(self, command):
        """
        Parses the modification command and applies it to the data.

        :param command: A SQL-like command for data modification.
        """
        self.data_modifier.parse_command(command)
        # write the change to csv
        if self.file_manager.data_modified:
            self.file_manager.write()


# Initliaze CSVDatabase
csv_database = CSVDatabase('./test/test_dataset.csv')

# Route to handle queries and modify data
@app.route('/', methods=['GET'])
def handle_request():
    """
    Handles incoming requests for querying or modifying the CSV data.
    Expects either a 'query' or 'job' parameter in the URL.

    :return: JSON response with the result of the query or modification.
    """
    query = request.args.get('query')
    job = request.args.get('job')

    if query:
        results = csv_database.query_data(query)
        # print(query, results)
        return jsonify({'result': results})
    elif job:
        csv_database.modify_data(job)
        return jsonify({'result': 'Success'})
    else:
        return jsonify({'msg': 'No valid parameters provided'}), 400


# Main function to start the server
if __name__ == '__main__':
    # app.run(debug=True, port=9527)
    app.run(debug=True, host='0.0.0.0', port=5000)