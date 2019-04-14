import unittest
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime
import meerkat_abacus
from meerkat_abacus.config import config
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps import quality_control
from meerkat_abacus.consumer.database_setup import create_db


# TODO: Test deviceid and exclusion list

class TestQualityControll(unittest.TestCase):

    def setUp(self):
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def test_quality_control(self):
        variables = [
            model.AggregationVariables(
                id="qul_1",
                type="import",
                form="demo_case",
                db_column="results./bmi_height",
                method="between",
                calculation="results./bmi_height",
                condition="50,220"
            ),
            model.AggregationVariables(
                id="qul_2",
                type="import",
                form="demo_case",
                db_column="pt./visit_date",
                method="between",
                category=["discard"],
                calculation='Variable.to_date(pt./visit_date)',
                condition="1388527200,2019679200"
            ),
            model.AggregationVariables(
                id="qul_3",
                type="import",
                form="demo_case",
                db_column="pt./visit_date2",
                method="match",
                category=["replace:SubmissionDate"],
                condition="15-Apr-2018"
            )

        ]
        config.country_config["quality_control"] = ["demo_case"]
        self.session.query(model.AggregationVariables).delete()
        self.session.commit()
        for v in variables:
            self.session.add(v)
        self.session.commit()

        qc = quality_control.QualityControl(
            config,
            self.session
        )

        data = {
            "meta/instanceID": 1,
            "deviceid": "1",
            "SubmissionDate": "2016-04-17T02:43:31.306860",
            "pt./visit_date": "2016-04-17",
            "results./bmi_height": 60,
            "intro./visit": "new"
        }

        result = qc.run("demo_case", data)[0]
        self.assertEqual(result["data"]["results./bmi_height"], 60)

        data["results./bmi_height"] = 20
        result = qc.run("demo_case", data)[0]
        self.assertEqual(result["data"]["results./bmi_height"], None)

        data["result./bmi_height"] = 220
        result = qc.run("demo_case", data)[0]
        self.assertEqual(result["data"]["results./bmi_height"], None)

        data["pt./visit_date"] = "15-Apr-2010"
        result = qc.run("demo_case", data)
        self.assertEqual(result, [])

        data["pt./visit_date"] = "15-Apr-2016"
        data["pt./visit_date2"] = "15-Apr-2019"
        result = qc.run("demo_case", data)[0]
        self.assertEqual(result["data"]["pt./visit_date2"],
                         "2016-04-17T02:43:31.306860")

        







class ValidateDateToEpiWeekConversionTest(unittest.TestCase):
    test_data_types_list = [{"date": "date_column"}]
    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_validates_proper_date(self, mock):
        test_row = {"date_column": "2017-01-01"}
        self.assertTrue(quality_control._validate_date_to_epi_week_convertion("test_form",
                                                                              test_row,
                                                                              config))

    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_bypass_for_missing_date(self, mock):
        test_row = {"date_column": ''}
        self.assertFalse(quality_control._validate_date_to_epi_week_convertion("test_form",
                                                                               test_row,
                                                                               config))

    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_bypass_and_logs_incorrect_date(self, mock):
        test_row = {"deviceid": "fake_me", "date_column": '31 Feb 2011'}
        with self.assertLogs(logger=meerkat_abacus.logger, level='DEBUG') as logs:
            quality_control._validate_date_to_epi_week_convertion("test_form", test_row,
                                                                  config)
            self.assertTrue(len(logs.output))
            self.assertIn("Failed to process date column for row with device_id: fake_me", logs.output[0])

    multiple_data_types_single_date = [
        {
            "db_column": "condition1",
            "condition": "valid",
            "date": "same_date"
        },
        {
            "date": "same_date"
        }
    ]

    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=multiple_data_types_single_date)
    def test_dates_should_be_tested_once(self, mock):
        test_row = {
            "condition1": "valid",
            "same_date": "June 14, 2015"
        }
        with patch.object(quality_control, 'epi_week_for_date') as mock:
            quality_control._validate_date_to_epi_week_convertion("test_form", test_row,
                                                              param_config=config)
            mock.assert_called_once()
            mock.assert_called_with(datetime(2015, 6, 14), param_config=config.country_config)

    test_epi_config = ({2015: datetime(2015, 3, 5)},)


    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=test_data_types_list)
    def test_bypass_if_date_out_of_custom_epi_config(self, data_types_mock):
        test_row = {"deviceid": "fake_me", "date_column": "03-05-2014"}

        config.country_config["epi_week"] = self.test_epi_config[0]
        with self.assertLogs(logger=meerkat_abacus.logger, level='DEBUG') as logs:
            quality_control._validate_date_to_epi_week_convertion("test_form", test_row,
                                                              param_config=config)
            self.assertTrue(len(logs.output))
            print(logs)
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

    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=test_multiple_data_types)
    def test_multiple_data_types_with_valid_dates(self, mock):
        test_row = {
            "condition1": "valid",
            "first_date": "May 5,2015",
            "condition2": "valid",
            "second_date": "June 14, 2015"
        }
        config.country_config["epi_week"] = self.test_epi_config[0]
        self.assertTrue(quality_control._validate_date_to_epi_week_convertion("test_form",
                                                                          test_row,
                                                                          param_config=config))

    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=test_multiple_data_types)
    def test_multiple_data_types_fails_if_single_date_invalid(self, mock):
        test_row = {
            "condition1": "valid",
            "first_date": "May 5,2015",
            "condition2": "valid",
            "second_date": "June 14, 2014"
        }
        config.country_config["epi_week"] = self.test_epi_config[0]
        self.assertFalse(quality_control._validate_date_to_epi_week_convertion("test_form",
                                                                           test_row,
                                                                           param_config=config))

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
    @patch.object(quality_control.data_types, 'data_types_for_form_name', return_value=data_types_mixed_condition)
    def test_multiple_data_types_passes_for_mixed_conditions(self, mock):
        test_row = {
            "condition1": "valid",
            "first_date": "May 5,2015",
            "second_date": "June 14, 2015"
        }
        self.assertTrue(quality_control._validate_date_to_epi_week_convertion("test_form", test_row,
                                                                              config))


if __name__ == "__main__":
    unittest.main()
