from flask import Flask, request, jsonify
import csv, re

app = Flask(__name__)

# load CSV data
def load_csv_data(file_path):
    data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

# parse the query into column name, operator, and value
def parse_query(query_str):
    conditions = re.split(r'\s+and\s+|\s+or\s+', query_str)
    parsed_conditions = []

    for condition in conditions:
        # apply regex expression
        match = re.match(r'(\*|[A-Za-z0-9_]+)\s*(==|!=|\$=|&=)\s*"((?:\\.|[^"\\])*)"', condition)
        if match:
            column, operator, value = match.groups()
            # Unescape any escaped quotes within the value
            value = value.replace('\\"', '"')
            parsed_conditions.append((column, operator, value))
        else:
            # error handling
            print("Error parsing condition:", condition)
            pass
    return parsed_conditions


# filter data to fetch rows match conditions
def filter_data(data, conditions):
    filtered_data = []
    print(data, conditions)
    for row in data:
        match = all(check_condition(row, condition) for condition in conditions)
        if match:
            filtered_data.append(row)
    return filtered_data

def check_condition(row, condition):
    column, operator, value = condition
    cell_value = row.get(column, "")  # use get to aovid key error
    if operator == '==':
        return cell_value == value
    elif operator == '!=':
        return cell_value != value
    elif operator == '$=':
        return cell_value.lower() == value.lower()
    elif operator == '&=':
        return value in cell_value
    return False



# Route to handle queries
@app.route('/', methods=['GET'])
def query_data():
    query = request.args.get('query')
    if query:
        conditions = parse_query(query)
        csv_data = load_csv_data('test_data.csv')
        results = filter_data(csv_data, conditions)
        return jsonify({'query': query, 'results': results})
    else:
        return jsonify({'error': 'No query provided'}), 400

# Start the server
if __name__ == '__main__':
    # app.run(debug=True, port=9527)
    
    csv_data = load_csv_data('test_data.csv')
    query = 'C1 == "Sample Text 1" and C2 == "Another \\"Sample\\""'

    conditions = parse_query(query)
    print(conditions)
    results = filter_data(csv_data, conditions)
    print(results)