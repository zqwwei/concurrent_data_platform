import unittest
from main import app  
from csv_file_manager import CSVFileManager
import os

class IntegrationTestCSVDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 创建测试用的 CSV 文件
        cls.test_csv_file = './test_data.csv'
        with open(cls.test_csv_file, 'w') as f:
            f.write("C1,C2,C3\nSample Text 1,Another Sample,Value 1\n")

        # 获取 Flask 应用的测试客户端
        cls.client = app.test_client()

        # 在这里，我们假设 Flask 应用可以通过配置读取 CSV 文件路径
        # 因此，我们在测试环境中设置 CSV 文件的路径
        app.config['CSV_FILE_PATH'] = cls.test_csv_file

    @classmethod
    def tearDownClass(cls):
        # 测试完成后删除测试用的 CSV 文件
        os.remove(cls.test_csv_file)

    def test_query_endpoint(self):
        # 测试查询端点
        response = self.client.get('/?query=C1 == "Sample Text 1"')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Sample Text 1', response.get_data(as_text=True))

    # def test_modify_endpoint(self):
    #     # 测试修改端点
    #     modify_response = self.app.get('/?job=INSERT "New Text","New Sample","New Value"')
    #     self.assertEqual(modify_response.status_code, 200)
    #     # 验证数据是否插入
    #     query_response = self.app.get('/?query=C1 == "New Text"')
    #     self.assertIn('New Text', query_response.get_data(as_text=True))

if __name__ == '__main__':
    unittest.main()