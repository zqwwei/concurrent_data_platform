import unittest
from app import app, load_csv_data

class FlaskTestCase(unittest.TestCase):

    # test csv data load
    def test_csv_loading(self):
        data = load_csv_data('test_data.csv')
        print(data)
        self.assertIsNotNone(data)  # 确保数据不为空

    # test query route 
    def test_query_route(self):
        tester = app.test_client(self)
        response = tester.get('/', query_string={'query': 'test_query'})
        self.assertEqual(response.status_code, 200)  
        self.assertIn(b'test_query', response.data) 

if __name__ == "__main__":
    unittest.main()
