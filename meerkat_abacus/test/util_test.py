"""
Testing for DB utilities
"""

import unittest
from datetime import datetime
from meerkat_abacus.util import create_fake_data, epi_week_start_date

class UtilTest(unittest.TestCase):

    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    def test_epi_week_start_date(self):
        first = datetime(2015, 1, 1)
        assert first == epi_week_start_date(2015, "international")

        first_saturday = datetime(2016,1,2)
        assert first_saturday == epi_week_start_date(2016, "day:5")

        first_tuesday = datetime(2016,1,5)
        assert first_tuesday == epi_week_start_date(2016, "day:1")

        first_wednesday = datetime(2016,1,6)
        assert first_wednesday == epi_week_start_date(2016, "day:2")
    
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
        assert records[-1]["index"] == 49
        
if __name__ == "__main__":
    unittest.main()
