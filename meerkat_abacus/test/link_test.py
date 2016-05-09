"""
Testing for DB utilities
"""

import unittest
from meerkat_abacus.data_management import prepare_link_data

class LinkTest(unittest.TestCase):
    """
    Test links
    """

    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    def test_prepare_link_data(self):
        data_def = {
            "key1": {
                "A": {
                    "column": "col1",
                    "condition": ["A", "B"]
                },
                "B": {
                    "column": "col1",
                    "condition": "B"
                },
                "C": {"condition": "default_value"}
            }
        }
        row1 = {"col1": "A"}
        new_data = prepare_link_data(data_def, row1)
        self.assertIn("key1", new_data.keys())
        self.assertEqual(new_data["key1"], "A")
        row1 = {"col1": "B"}
        new_data = prepare_link_data(data_def, row1)
        self.assertEqual(sorted(new_data["key1"]), ["A", "B"])
        row1 = {"col1": "C"}
        new_data = prepare_link_data(data_def, row1)
        self.assertEqual(new_data["key1"], "C")
        data_def = {
            "key1": {
                "A": {
                    "column": "col1",
                    "condition": "get_value"
                }
            }
        }
        row1 = {"col1": "D"}
        new_data = prepare_link_data(data_def, row1)
        self.assertEqual(new_data["key1"], "D")
        
        
if __name__ == "__main__":
    unittest.main()
