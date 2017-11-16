import unittest
from unittest.mock import patch

from datetime import datetime

import meerkat_abacus.data_management as data_management


class ValidateDateToEpiWeekConversionTest(unittest.TestCase):
    test_data_types_list = [{"date": "date_column"}]

    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_validates_proper_date(self, mock):
        test_row = {"date_column": "2017-01-01"}
        self.assertTrue(data_management._validate_date_to_epi_week_convertion("test_form", test_row))

    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_bypass_for_missing_date(self, mock):
        test_row = {"date_column": ''}
        self.assertFalse(data_management._validate_date_to_epi_week_convertion("test_form", test_row))

    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_bypass_and_logs_incorrect_date(self, mock):
        test_row = {"deviceid": "fake_me", "date_column": '31 Feb 2011'}
        with self.assertLogs(level='DEBUG') as logs:
            data_management._validate_date_to_epi_week_convertion("test_form", test_row)
            self.assertTrue(len(logs))
            self.assertIn("Failed to process date column for row with device_id: fake_me", logs.output[0])

    test_epi_config = ({2015: datetime(2015, 3, 5)},)

    @patch('meerkat_abacus.util.epi_week.epi_year_start_date.__defaults__', new=test_epi_config)
    @patch('meerkat_abacus.util.epi_week.epi_year_by_date.__defaults__', new=test_epi_config)
    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_bypass_if_date_out_of_custom_epi_config(self, data_types_mock):
        test_row = {"deviceid": "fake_me", "date_column": "03-05-2014"}
        with self.assertLogs(level='DEBUG') as logs:
            data_management._validate_date_to_epi_week_convertion("test_form", test_row)
            self.assertTrue(len(logs))
            self.assertIn("Failed to process date column for row with device_id: fake_me", logs.output[0])

    test_multiple_data_types = [
        {
            "db_column": "condition1",
            "condition": "valid",
            "date": "first_date"
        },
        {
            "db_column": "condition2",
            "condition": "valid",
            "date": "second_date"
        }
    ]

    @patch('meerkat_abacus.util.epi_week.epi_year_start_date.__defaults__', new=test_epi_config)
    @patch('meerkat_abacus.util.epi_week.epi_year_by_date.__defaults__', new=test_epi_config)
    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=test_multiple_data_types)
    def test_multiple_data_types_with_valid_dates(self, mock):
        test_row = {
            "condition1": "valid",
            "first_date": "May 5,2015",
            "condition2": "valid",
            "second_date": "June 14, 2015"
        }
        self.assertTrue(data_management._validate_date_to_epi_week_convertion("test_form", test_row))

    @patch('meerkat_abacus.util.epi_week.epi_year_start_date.__defaults__', new=test_epi_config)
    @patch('meerkat_abacus.util.epi_week.epi_year_by_date.__defaults__', new=test_epi_config)
    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=test_multiple_data_types)
    def test_multiple_data_types_fails_if_single_date_invalid(self, mock):
        test_row = {
            "condition1": "valid",
            "first_date": "May 5,2015",
            "condition2": "valid",
            "second_date": "June 14, 2014"
        }
        self.assertFalse(data_management._validate_date_to_epi_week_convertion("test_form", test_row))

    data_types_mixed_condition = [
        {
            "db_column": "condition1",
            "condition": "valid",
            "date": "first_date"
        },
        {
            "date": "second_date"
        }
    ]

    @patch('meerkat_abacus.util.epi_week.epi_year_start_date.__defaults__', new=test_epi_config)
    @patch('meerkat_abacus.util.epi_week.epi_year_by_date.__defaults__', new=test_epi_config)
    @patch.object(data_management.data_types, 'data_types_for_form_name', return_value=data_types_mixed_condition)
    def test_multiple_data_types_passes_for_mixed_conditions(self, mock):
        test_row = {
            "condition1": "valid",
            "first_date": "May 5,2015",
            "second_date": "June 14, 2015"
        }
        self.assertTrue(data_management._validate_date_to_epi_week_convertion("test_form", test_row))


if __name__ == "__main__":
    unittest.main()
