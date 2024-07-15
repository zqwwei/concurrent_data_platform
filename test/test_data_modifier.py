# import unittest
# from unittest.mock import MagicMock
# from data_modifier import DataModifier  # Replace with your actual import

# class TestDataModifier(unittest.TestCase):
#     def setUp(self):
#         # create MagicMock to mimic file_manager
#         self.mock_file_manager = MagicMock()
#         self.mock_file_manager.data = [
#             {"C1": "Value1", "C2": "Value2", "C3": "Value3"},
#             {"C1": "Value4", "C2": "Value5", "C3": "Value6"}
#         ]
#         self.data_modifier = DataModifier(self.mock_file_manager)

#     def test_parse_insert_valid_command(self):
#         command = 'INSERT "Value1", "Value2", "Value3"'
#         self.data_modifier.parse_command(command)
#         # 检查 insert 方法是否被正确调用一次
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(len(self.mock_file_manager.data), 3)

#     def test_parse_delete_valid_command(self):
#         command = 'DELETE "Value1"'
#         self.data_modifier.parse_command(command)
#         # 检查 delete 方法是否正确减少了 data 中的元素数量
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(len(self.mock_file_manager.data), 1)

#     def test_parse_update_valid_command(self):
#         command = 'UPDATE "Value1", "Value2" C2, "UpdatedValue"'
#         self.data_modifier.parse_command(command)
#         # 检查 update 方法是否正确更新了 data 中的值
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(self.mock_file_manager.data[0]["C2"], "UpdatedValue")

#     def test_parse_insert_invalid_command(self):
#         # 测试无效的 INSERT 命令格式
#         command = 'INSERT "OnlyOneValue"'
#         with self.assertRaises(ValueError):
#             self.data_modifier.parse_command(command)

#     def test_parse_delete_invalid_command(self):
#         # 测试无效的 DELETE 命令格式
#         command = 'DELETE THIS'
#         with self.assertRaises(ValueError):
#             self.data_modifier.parse_command(command)

#     def test_parse_update_invalid_command(self):
#         # 测试无效的 UPDATE 命令格式
#         command = 'UPDATE "NoValues"'
#         with self.assertRaises(ValueError):
#             self.data_modifier.parse_command(command)
        
#     def test_parse_insert_with_extra_spaces(self):
#         command = 'INSERT  "new" ,  "new" ,  "new" '
#         self.data_modifier.parse_command(command)
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(len(self.mock_file_manager.data), 3)

#     def test_parse_delete_with_extra_spaces(self):
#         command = 'DELETE  "Value1" '
#         self.data_modifier.parse_command(command)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(len(self.mock_file_manager.data), 1)
        
#     def test_parse_update_nonexistent_column(self):
#         command = 'UPDATE "Value1", NonexistentColumn, "UpdatedValue"'
#         with self.assertRaises(ValueError):
#             self.data_modifier.parse_command(command)

#     def test_parse_delete_multiple_conditions(self):
#         command = 'DELETE "Value1", "Value2"'
#         self.data_modifier.parse_command(command)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(len(self.mock_file_manager.data), 1)

#     def test_parse_update_multiple_conditions(self):
#         command = 'UPDATE "Value1", "Value2", C3, "UpdatedValue"'
#         self.data_modifier.parse_command(command)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(self.mock_file_manager.data[0]["C3"], "UpdatedValue")
        
#     def test_parse_insert_empty_values(self):
#         command = 'INSERT "", "", ""'
#         self.data_modifier.parse_command(command)
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(len(self.mock_file_manager.data), 3)
        
#     def test_parse_delete_empty_value(self):
#         command = 'DELETE ""'
#         self.mock_file_manager.data_modified = False
#         self.data_modifier.parse_command(command)
#         # print(self.mock_file_manager.data)
#         self.assertFalse(self.mock_file_manager.data_modified)
        
#     def test_parse_update_empty_value(self):
#         command = 'UPDATE "Value1", "C2", ""'
#         self.data_modifier.parse_command(command)
#         self.assertTrue(self.mock_file_manager.data_modified)
#         self.assertEqual(self.mock_file_manager.data[0]["C2"], "")
        
#     def test_parse_insert_special_characters(self):
#         command = r'INSERT "Value \"One\"", "Value, Two", "Value/Three"'
#         self.data_modifier.parse_command(command)
#         self.assertTrue(self.mock_file_manager.data_modified)
        
#     def test_parse_delete_special_characters(self):
#         command = r'DELETE "Value \"One\""'
#         self.data_modifier.parse_command(command)
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)
        
#     def test_parse_update_special_characters(self):
#         command = r'UPDATE "Value1" "Value2" "C2" "\"One\""'
#         self.data_modifier.parse_command(command)
#         # print(self.mock_file_manager.data)
#         self.assertTrue(self.mock_file_manager.data_modified)

# # if __name__ == '__main__':
# #     unittest.main()