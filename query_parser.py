import re

class QueryParser:
    """
    A class for parsing SQL-like query strings into conditions.
    """

    def parse(self, query_str):
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

        return parsed_conditions