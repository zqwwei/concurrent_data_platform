import re

class DataFilter:
    """
    A class for filtering data rows based on specified conditions.
    Each condition can check a specific column or all columns (*) with support for
    different operators (==, !=, $= for case-insensitive match, and &= for containment check).
    Conditions can be combined with 'and'/'or' logical operations.
    """

    def __init__(self, file_manager):
        """
        Initializes a DataFilter instance with a reference to a CSVFileManager.

        :param file_manager: The CSVFileManager instance whose data will be filtered.
        """
        self.file_manager = file_manager
        self.parsed_conditions = None

    def parse_command(self, query_str):
        """
        Parses a query string into a list of conditions.

        Each condition is represented as a tuple containing the column name or '*',
        an operator ('==', '!=', '$=', '&='), the value to compare, and a logical
        operator ('and', 'or') if any, indicating the relation to the next condition.

        :param query_str: The query string to be parsed.
        :return: A list of tuples representing the parsed conditions.
        :raises ValueError: If the query string cannot be parsed.
        """
        # print(query_str)
        matches = re.findall(r'(\*|[A-Za-z0-9_]+)\s*(==|!=|\$=|&=)\s*"(.*?)(?<!\\)"(\s+and\s+|\s+or\s+|$)', query_str, re.DOTALL)
        # print(matches)
        if not matches:
            raise ValueError("Error parsing query: " + query_str)
        
        parsed_conditions = []
        for column, operator, value, logic in matches:
            # Handle escaped quotes
            value = value.replace('\\"', '"')
            parsed_conditions.append((column, operator, value, logic.strip())) # strip out space for and/or

        self.parsed_conditions = parsed_conditions
    
    def filter(self, command):
        """
        Filters data rows based on the parsed query conditions and returns a string
        representation of the filtered data rows, with values separated by commas and
        rows separated by newlines.

        :param conditions: A list of tuples representing conditions (column, operator, value, logic).
        :return: A string representing the filtered rows of data.
        """
        self.parsed_conditions = self.parse_command(command)
        
        # print(self.parsed_conditions)
        filtered_data = []
        for row in self.file_manager.data:
            match = False
            last_logic = 'and'
            for i, condition in enumerate(self.parsed_conditions):
                column, operator, value, logic = condition
                condition_match = self.check_condition(row, (column, operator, value))
                if last_logic == 'and':
                    match = condition_match if i == 0 else match and condition_match
                elif last_logic == 'or':
                    match = match or condition_match
                # reach end of condition, no logic, break
                if logic == '':
                    break
                last_logic = logic
            if match:
                filtered_data.append(row)
        
        rows_as_strings = [",".join(map(str, row.values())) for row in filtered_data]
        return "\n".join(rows_as_strings)

    def check_condition(self, row, condition):
        """
        Checks if a row matches a given condition.

        :param row: A dictionary representing a row of data.
        :param condition: A tuple representing a condition (column, operator, value).
        :return: Boolean indicating if the condition is met.
        """
        column, operator, value = condition
        if column == '*':
            return self.check_all_columns(row, operator, value)
        else:
            cell_value = row.get(column, "")
            return self.evaluate_condition(cell_value, operator, value)

    def check_all_columns(self, row, operator, value):
        """
        Checks all columns of a row to see if the condition is met in any of them.

        :param row: A dictionary representing a row of data.
        :param operator: The operator to use for comparison.
        :param value: The value to compare against.
        :return: Boolean indicating if the condition is met in any column.
        """
        for cell_value in row.values():
            if not self.evaluate_condition(cell_value, operator, value):
                return False
        return True

    def evaluate_condition(self, cell_value, operator, value):
        """
        Evaluates a condition based on the operator.

        :param cell_value: The value from the data row's cell.
        :param operator: The operator to use for comparison.
        :param value: The value to compare against.
        :return: Boolean indicating if the condition is met.
        """
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