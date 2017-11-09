import unittest

from datetime import datetime

from meerkat_abacus.util.epi_week import epi_year_start_date, epi_year_by_date


class EpiWeekTest(unittest.TestCase):
    def test_epi_year_start_for_international_config(self):
        expected_epi_year_start_date = datetime(2015, 1, 1)
        date = datetime(2015, 5, 25)
        self.assertEqual(expected_epi_year_start_date, epi_year_start_date(date, epi_config="international"))

    def test_epi_year_start_for_custom_weekday(self):
        year = 2016
        first_weekdays_in_year_days = [4, 5, 6, 7, 1, 2, 3]
        first_weekdays_in_year_datetimes = [datetime(year, 1, day) for day in first_weekdays_in_year_days]
        date = datetime(2016, 6, 14)

        for weekday, expected_epi_year_start_datetime in enumerate(first_weekdays_in_year_datetimes):
            test_config = "day:{!r}".format(weekday)
            actual_epi_year_start_datetime = epi_year_start_date(date, epi_config=test_config)
            self.assertEqual(expected_epi_year_start_datetime, actual_epi_year_start_datetime)

    def test_epi_year_start_for_custom_start_date(self):
        test_config = {
            2015: datetime(2015, 1, 1),
            2016: datetime(2016, 1, 2),
            2017: datetime(2016, 12, 30)
        }
        test_data = [
            {"date": datetime(2016, 1, 1), "expected_year": 2015},
            {"date": datetime(2016, 3, 5), "expected_year": 2016},
            {"date": datetime(2016, 12, 31), "expected_year": 2017},
            {"date": datetime(2017, 4, 24), "expected_year": 2017}
        ]
        for _test in test_data:
            expected_datetime = test_config[_test["expected_year"]]
            actual_datetime = epi_year_start_date(_test["date"], epi_config=test_config)
            failure_message = f"Failed for date: '{_test['date']}'."
            self.assertEqual(expected_datetime, actual_datetime, msg=failure_message)

    def test_epi_year_by_date_international_config(self):
        test_epi_config = "international"
        test_data = [
            {"date": datetime(2017, 3, 5), "expected_year": 2017},
            {"date": datetime(2017, 1, 1), "expected_year": 2017},
            {"date": datetime(2016, 12, 31), "expected_year": 2016}
        ]
        self.__assert_valid_year_for_dates(method_under_test=epi_year_by_date,
                                           test_config=test_epi_config,
                                           test_data=test_data)

    def test_epi_year_by_date_weekday_config(self):
        year = 2016
        test_epi_config = "day:5"
        test_data = [
            {"date": datetime(2016, 1, 1), "expected_year": 2015},
            {"date": datetime(2016, 1, 2), "expected_year": 2016},
            {"date": datetime(2017, 1, 6), "expected_year": 2016},
            {"date": datetime(2017, 1, 7), "expected_year": 2017},
            {"date": datetime(2017, 12, 31), "expected_year": 2017},

        ]
        self.__assert_valid_year_for_dates(method_under_test=epi_year_by_date,
                                           test_config=test_epi_config,
                                           test_data=test_data)

    def test_epi_year_by_date_custom_config(self):
        test_config = {
            2015: datetime(2015, 1, 1),
            2016: datetime(2016, 1, 2),
            2017: datetime(2016, 12, 30)
        }
        test_data = [
            {"date": datetime(2016, 1, 1), "expected_year": 2015},
            {"date": datetime(2016, 3, 5), "expected_year": 2016},
            {"date": datetime(2016, 12, 31), "expected_year": 2017},
            {"date": datetime(2017, 4, 24), "expected_year": 2017}
        ]
        self.__assert_valid_year_for_dates(method_under_test=epi_year_by_date,
                                           test_config=test_config,
                                           test_data=test_data)

    def __assert_valid_year_for_dates(self, method_under_test, test_config, test_data):
        for _test in test_data:
            expected_year = _test["expected_year"]
            actual_year = method_under_test(_test["date"], epi_config=test_config)
            failure_message = f"Failed for config: '{test_config}' and date: '{_test['date']}'"
            self.assertEqual(expected_year, actual_year, msg=failure_message)
