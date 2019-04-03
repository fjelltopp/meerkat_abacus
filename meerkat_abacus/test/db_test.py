import unittest

import os
from sqlalchemy_utils import database_exists, drop_database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from shapely.geometry import Polygon
from unittest.mock import patch

import meerkat_abacus.util.epi_week
from meerkat_abacus import data_management as manage
from meerkat_abacus import model, util, tasks, data_import
from meerkat_abacus.config import config, Config
from geoalchemy2.shape import to_shape
import yaml


class DbTest(unittest.TestCase):
    """
    Test setting up database functionality
    """

    current_directory = os.path.dirname(os.path.realpath(__file__))

    def setUp(self):
        manage.create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.conn = self.engine.connect()
        self.current_directory = current_directory = os.path.dirname(os.path.realpath(__file__))
        self.param_config_yaml = yaml.dump(config)

    def tearDown(self):
        self.session.commit()
        self.session.close()
        self.engine.dispose()
        #if database_exists(config.DATABASE_URL):
        #    drop_database(config.DATABASE_URL)

    def test_create_db(self):
        if database_exists(config.DATABASE_URL):
            drop_database(config.DATABASE_URL)
        self.assertFalse(database_exists(config.DATABASE_URL))
        manage.create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.Base.metadata.create_all(engine)
        self.assertTrue(database_exists(config.DATABASE_URL))

    def test_locations(self):
        old_dir = manage.config.config_directory
        manage.config.config_directory = self.current_directory + "/test_data/"
        old_locs = manage.country_config["locations"]
        manage.country_config["locations"] = {
            "clinics": "demo_clinics.csv",
            "districts": "demo_districts.csv",
            "regions": "demo_regions.csv",
            "zones": "demo_zones.csv"
        }
        manage.import_locations(self.engine, self.session)
        results = self.session.query(model.Locations).all()
        self.assertEqual(
            len(results),
            14)  # Important as we should not merge the final Clinic 1
        for r in results:
            # Checking that devideids are handled properly
            if r.name == "Clinic 1":
                if r.parent_location == 6:
                    self.assertEqual(r.deviceid, "1,6")
                    self.assertTrue(r.case_report)
                else:
                    self.assertEqual(r.deviceid, "7")

            if r.name == "Clinic 2":
                self.assertEqual(r.start_date, datetime(2016, 2, 2))
            elif r.level == "clinic":
                self.assertEqual(
                    r.start_date,
                    manage.config.country_config["default_start_date"])
            if r.name == "District 1":
                self.assertEqual(
                    list(Polygon([(0, 0), (0, 0.4), (0.2, 0.4), (0.2, 0), (0, 0)]).exterior.coords),
                    list(to_shape(r.area).geoms[0].exterior.coords)
                )

        # This file has a clinic with a non existent district
        old_clinic_file = manage.country_config["locations"]["clinics"]
        manage.country_config["locations"][
            "clinics"] = "demo_clinics_error.csv"
        with self.assertRaises(KeyError):
            manage.import_locations(self.engine, self.session)

        # Clean Up
        manage.country_config["locations"]["clinics"] = old_clinic_file
        manage.country_config["locations"] = old_locs
        manage.config.config_directory = old_dir

    country_config_mock = {
        "locations": {
            "clinics": "demo_clinics.csv",
            "districts": "demo_districts.csv",
            "regions": "demo_regions.csv",
            "zones": "demo_zones.csv"
        },
        "types_file": "data_types_multi.csv",
        "tables": ["demo_case"],
        "epi_week": "international",
        "links_file": "demo_links.csv"
    }

    @patch.object(manage.data_types, "DATA_TYPES_DICT", new=None)
    def test_multiple_rows_in_a_row(self):

        self.session.query(model.AggregationVariables).delete()
        self.session.commit()
        self.session.add(model.AggregationVariables(
            id="b_1",
            type="case",
            form="demo_case",
            db_column="b",
            method="match",
            calculation="results./bmi_height",
            condition="test1,test2",
            category="test"
        ))
        self.session.commit()

        manage.import_locations(self.engine, self.session)

        data_row = {"a": "test", "b1": "test1", "b2": "test2",
                    "pt./visit_date": "2016/01/01", "meta/instanceID": "1",
                    "deviceid": "1"}
        self.session.query(model.form_tables()["demo_case"]).delete()
        self.session.query(model.Data).delete()
        self.session.commit()

        case = model.form_tables()["demo_case"](
            uuid="hei",
            data=data_row
        )
        self.session.add(case)
        self.session.commit()
        config = Config()
        config.config_directory = self.current_directory + "/test_data/"
        config.country_config["types_file"] = "data_types_multi.csv"
        # from nose.tools import set_trace; set_trace()
        manage.new_data_to_codes(self.engine, param_config=config)

        N_cases = self.session.query(model.Data).count()

        self.assertEqual(N_cases, 2)

    def test_table_data_from_csv(self):
        """Test table_data_from_csv"""

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
                db_column="pt./visit_date",
                method="match",
                category=["replace:SubmissionDate"],
                condition="15-Apr-2016"
            )

        ]

        self.session.query(model.AggregationVariables).delete()
        self.session.commit()
        for v in variables:
            self.session.add(v)
        self.session.commit()

        form_data = []
        for d in util.read_csv(self.current_directory + "/test_data/" + "demo_case.csv"):
            form_data.append(d)

        data_import.add_rows_to_db(
            "demo_case",
            form_data,
            self.session,
            self.engine,
            deviceids=["1", "2", "3", "4", "5", "6"],
            start_dates={"2": datetime(2016, 2, 2)},
            table_name="demo_case",
            quality_control=True)
        results = self.session.query(model.form_tables()["demo_case"]).all()
        self.assertEqual(len(results), 4)
        # Only 6 of the cases have deviceids in 1-6
        # One has to early submission date and one
        # is discarded by qul_2 above

        for r in results:
            if r.uuid in ["1", "3"]:
                self.assertEqual(r.data["results./bmi_height"], None)
                self.assertNotEqual(r.data["results./bmi_weight"], None)
            else:
                self.assertNotEqual(r.data["results./bmi_height"], None)
                self.assertNotEqual(r.data["results./bmi_weight"], None)
            if r.uuid == "5":
                self.assertEqual(r.data["pt./visit_date"], "2016-04-17T02:43:31.306860")
            self.assertIn(r.uuid, ["3", "4", "5", "1"])

    def test_db_setup(self):
        old_manual = tasks.config.country_config["manual_test_data"]
        tasks.config.country_config["manual_test_data"] = {}
        tasks.set_up_db.apply(kwargs={"param_config_yaml": self.param_config_yaml}).get()
        tasks.initial_data_setup(source="FAKE_DATA",
                                 param_config_yaml=self.param_config_yaml)
        self.assertTrue(database_exists(config.DATABASE_URL))
        engine = self.engine
        session = self.session
        # Locations
        results = session.query(model.Locations)

        if config.fake_data:
            for table in model.form_tables():
                results = session.query(model.form_tables()[table])
                self.assertEqual(len(results.all()), 500)

        # Import variables
        agg_var = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.id == "tot_1").first()
        self.assertEqual(agg_var.name, "Total")

        # Number of cases

        n_cases = len(
            session.query(model.Data).filter(model.Data.type == "case").all())

        n_disregarded_cases = len(
            session.query(model.DisregardedData).filter(model.Data.type == "case").all())

        t = model.form_tables()[config.country_config["tables"][0]]
        n_expected_cases = len(
            session.query(t).filter(t.data["intro./visit"].astext == "new")
                .all())
        self.assertEqual(n_cases + n_disregarded_cases, n_expected_cases)

        agg_var_female = "gen_2"
        results = session.query(model.Data).filter(model.Data.type == "case")
        results2 = session.query(model.DisregardedData).filter(model.DisregardedData.type == "case")
        number_of_totals = 0
        number_of_female = 0
        for row in results:
            epi_year, epi_week = meerkat_abacus.util.epi_week.epi_week_for_date(row.date)
            self.assertEqual(epi_week, row.epi_week)
            self.assertEqual(epi_year, row.epi_year)
            if "tot_1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female) in row.variables.keys():
                number_of_female += 1
        for row in results2:
            if "tot_1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female) in row.variables.keys():
                number_of_female += 1

        total = session.query(t).filter(
            t.data.contains({"intro./visit": "new"}))
        female = session.query(t).filter(
            t.data.contains({"intro./visit": "new",
                             "pt1./gender": "female"
                             }))
        self.assertEqual(number_of_totals, len(total.all()))
        self.assertEqual(number_of_female, len(female.all()))
        session.close()
        tasks.config.country_config["manual_test_data"] = old_manual

    def test_get_proccess_data(self):
        old_fake = tasks.config.fake_data
        old_s3 = tasks.config.get_data_from_s3
        tasks.config.fake_data = True
        tasks.config.get_data_from_s3 = False
        old_manual = tasks.config.country_config["manual_test_data"]
        tasks.config.country_config["manual_test_data"] = {}
        numbers = {}
        tasks.set_up_db.apply(kwargs={"param_config_yaml": self.param_config_yaml}).get()
        for table in model.form_tables():
            res = self.session.query(model.form_tables()[table])
            numbers[table] = len(res.all())
        tasks.add_fake_data()
        tasks.process_buffer(start=False)
        for table in model.form_tables():
            res = self.session.query(model.form_tables()[table])
            self.assertEqual(numbers[table] + 10, len(res.all()))
        #Clean up
        tasks.config.fake_data = old_fake
        tasks.config.get_data_from_s3 = old_s3
        tasks.config.country_config["manual_test_data"] = old_manual

    def test_get_new_data_initial_visit_control(self):
        """
        Tests the initial visit control in case new data is brought in and it needs to be validated
        """
        old_fake = tasks.config.fake_data
        old_s3 = tasks.config.get_data_from_s3
        tasks.config.fake_data = True
        tasks.config.get_data_from_s3 = False
        manage.create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.Base.metadata.create_all(engine)

        numbers = {}
        manage.import_locations(self.engine, self.session)
        manage.import_variables(self.session)
        manage.add_fake_data(self.session, N=500, append=False)
        for table in model.form_tables():
            res = self.session.query(model.form_tables()[table])
            numbers[table] = len(res.all())
        tasks.add_fake_data()
        tasks.process_buffer(start=False)
        for table in model.form_tables():
            res = self.session.query(model.form_tables()[table])
            self.assertEqual(numbers[table] + 10, len(res.all()))
        #Reset configuration parameters
        tasks.config.fake_data = old_fake
        tasks.config.get_data_from_s3 = old_s3


if __name__ == "__main__":
    unittest.main()
