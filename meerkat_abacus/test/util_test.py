"""
Testing for DB utilities
"""

import unittest
from meerkat_abacus.database_util import create_fake_data

class DbUtilTest(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def test_create_fake_data_get_value(self):
        """Test get value"""
        value = create_fake_data.get_value(
            {"integer": [1, 2]}, None)
        assert value >= 1
        assert value <= 2
        value = create_fake_data.get_value(
            {"one": ["one", "two"]}, None)
        assert value in ["one", "two"]
        data = {"deviceids": [1, 2, 3, 4, 5]}
        value = create_fake_data.get_value(
            {"data": "deviceids"}, data)
        assert value in data["deviceids"]

    def test_create_fake_data(self):
        """
        Test creating form data
        """
        fields = {"a": {"integer": [1, 3]},
                  "b": {"one": ["one", "two"]}}
        records = create_fake_data.create_form(fields, N=50, odk=False)
        assert len(records) == 50
        assert "a" in records[0].keys()
        assert "b" in records[0].keys()
        data = {"deviceids": [1, 3]}
        records = create_fake_data.create_form(fields,
                                               N=50,
                                               data=data,
                                               odk=True)
        assert "deviceid" in records[0].keys()
        assert "start" in records[0].keys()
        assert records[-1]["_index"] == 49
        
if __name__ == "__main__":
    unittest.main()
