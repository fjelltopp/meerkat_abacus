import unittest
from datetime import datetime
from unittest.mock import patch

import meerkat_abacus.data_management as data_management

class ValidateDateToEpiWeekConversionTest(unittest.TestCase):
    def setUp(self):
        pass

    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value={"date": "date_column"})
    def test_validates_proper_date(self, mock):
        test_row = {"date_column": "2017-01-01"}
        self.assertTrue(data_management._validate_date_to_epi_week_convertion("test_form", test_row))

    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value={"date": "date_column"})
    def test_bypass_for_missing_date(self, mock):
        test_row = {"date_column": ''}
        self.assertFalse(data_management._validate_date_to_epi_week_convertion("test_form", test_row))

    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value={"date": "date_column"})
    def test_bypass_and_logs_incorrect_date(self, mock):
        test_row = {"deviceid": "fake_me", "date_column": '31 Feb 2011'}
        with self.assertLogs(level='WARNING') as logs:
            data_management._validate_date_to_epi_week_convertion("test_form", test_row)
            self.assertTrue(len(logs))
            self.assertIn("Failed to process date column for row with device_id: fake_me", logs.output[0])

    test_epi_config = ({2015: datetime(2015, 3, 5)},)

    @patch('meerkat_abacus.util.epi_week.epi_year_start_date.__defaults__', new=test_epi_config)
    @patch('meerkat_abacus.util.epi_week.epi_year_by_date.__defaults__', new=test_epi_config)
    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value={"date": "date_column"})
    def test_bypass_if_date_out_of_custom_epi_config(self, data_types_mock):
        test_row = {"deviceid": "fake_me", "date_column": "03-05-2014"}
        with self.assertLogs(level='WARNING') as logs:
            data_management._validate_date_to_epi_week_convertion("test_form", test_row)
            self.assertTrue(len(logs))
            self.assertIn("Failed to process date column for row with device_id: fake_me", logs.output[0])

