from datetime import datetime
import unittest

from util import epi_year_start_date


class EpiWeekTest(unittest.TestCase):
    def test_epi_year_start_for_international_config(self):
        first_day_of_year = datetime(2015, 1, 1)
        self.assertEqual(first_day_of_year, epi_year_start_date(2015, "international"))

    def test_epi_year_start_for_custom_weekday(self):
        year = 2016
        first_weekdays_in_year_days = [4, 5, 6, 7, 1, 2, 3]
        first_weekdays_in_year_datetimes = [datetime(year, 1, day) for day in first_weekdays_in_year_days]

        for weekday, expected_epi_year_start_datetime in enumerate(first_weekdays_in_year_datetimes):
            epi_config = "day:{!r}".format(weekday)
            actual_epi_year_start_datetime = epi_year_start_date(year, epi_config)
            self.assertEqual(expected_epi_year_start_datetime, actual_epi_year_start_datetime)

    def test_epi_year_start_for_custom_start_date(self):
        epi_config = {
            2016: datetime(2016, 1, 2),
            2017: datetime(2016, 12, 30)
        }
        for year in epi_config:
            expected_datetime = epi_config[year]
            actual_datetime = epi_year_start_date(year, epi_config)
            self.assertEqual(expected_datetime, actual_datetime)
