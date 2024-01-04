import unittest, json
from app import app, load_csv_data

class FlaskTestCase(unittest.TestCase):

    # test csv data load
    def test_csv_loading(self):
        data = load_csv_data('test_data.csv')
        # print(data)
        self.assertIsNotNone(data)  # 确保数据不为空

    # test query route 
    def test_query_route(self):
        tester = app.test_client(self)
        response = tester.get('/', query_string={'query': 'C1 == "Sample Text 1" and C2 == "Another \\"Sample\\""'})
        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.data)
        self.assertEqual(response_data['query'], 'C1 == "Sample Text 1" and C2 == "Another \\"Sample\\""')
        self.assertIn({'C1': 'Sample Text 1', 'C2': 'Another "Sample"', 'C3': 'Value 1'}, response_data['results'])


if __name__ == "__main__":
    unittest.main()
