# import unittest
# from data_filter import DataFilter
# from query_parser import QueryParser
# from unittest.mock import MagicMock

# class TestDataFilter(unittest.TestCase):
#     def setUp(self):
#         # create MagicMock to mimic file_manager
#         self.mock_file_manager = MagicMock()
#         self.mock_file_manager.data = [
#             {'Nation': 'A', 'Category': 'B', 'Entity': 'C'},
#             {'Nation': 'United StAtes', 'Category': 'Best Rapper', 'Entity': 'XXXTentacion'}
#         ]
#         self.filter = DataFilter(self.mock_file_manager)
#         self.parser = QueryParser()

#     def test_filter_equals(self):
#         conditions = self.parser.parse('Nation == "A"')
#         results = self.filter.filter(conditions)
#         self.assertEqual(results, "A,B,C")

#     def test_filter_equals2(self):
#         conditions = self.parser.parse('Nation &= "A"')
#         results = self.filter.filter(conditions)
#         self.assertEqual(results, "A,B,C\nUnited StAtes,Best Rapper,XXXTentacion")

#     def test_filter_contains(self):
#         # Test the containment operation
#         conditions = self.parser.parse('Nation &= "United"')
#         results = self.filter.filter(conditions)
#         expected_results = "United StAtes,Best Rapper,XXXTentacion"
#         self.assertEqual(results, expected_results)

#     def test_filter_case_insensitive_equals(self):
#         # Test case-insensitive equality operation
#         conditions = self.parser.parse('Nation $= "united states"')
#         results = self.filter.filter(conditions)
#         expected_results = "United StAtes,Best Rapper,XXXTentacion"
#         self.assertEqual(results, expected_results)

#     def test_filter_not_equals(self):
#         # Test the inequality operation
#         conditions = self.parser.parse('Nation != "A"')
#         results = self.filter.filter(conditions)
#         expected_results = "United StAtes,Best Rapper,XXXTentacion"
#         self.assertEqual(results, expected_results)


# # if __name__ == '__main__':
# #     unittest.main()
