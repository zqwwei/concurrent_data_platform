# import unittest
# from csv_manager import CSVFileManager
# import os
# import time

# class TestCSVFileManager(unittest.TestCase):
#     def setUp(self):
#         """
#         Set up the test environment before each test.
#         This includes specifying a test CSV file and initializing the CSVFileManager with a short write interval.
#         """
#         current_dir = os.path.dirname(os.path.abspath(__file__))
#         self.test_file = os.path.join(current_dir, 'test_data.csv')
#         self.manager = CSVFileManager(self.test_file, write_interval=1)  # short interval for faster testing

#     def test_read_function(self):
#         """
#         Test that the CSVFileManager correctly reads data from a CSV file.
#         Verifies that the data is not None and contains rows after reading.
#         """
#         self.assertIsNotNone(self.manager.data, "Data should not be None after reading")
#         self.assertTrue(len(self.manager.data) > 0, "Data should contain rows after reading")
#         print("read Successfully runs")

#     def test_write_function(self):
#         """
#         Test that the CSVFileManager correctly writes modified data back to the CSV file.
#         This is done by modifying the in-memory data, waiting for the background write to occur,
#         and then verifying that the new data is present when re-reading the file.
#         """
#         # Append a new row to the data and mark it as modified
#         self.manager.data.append({'C1': 'New Text', 'C2': 'New Sample', 'C3': 'New Value'})
#         self.manager.data_modified = True
        
#         time.sleep(2)  # Wait for background write to happen
        
#         # Reinitialize manager to read the updated file
#         new_manager = CSVFileManager(self.test_file)
#         self.assertTrue(any(row['C1'] == 'New Text' for row in new_manager.data), "New data should be written to the file")
#         print("writes Successfully runs")

# # if __name__ == '__main__':
# #     unittest.main()
