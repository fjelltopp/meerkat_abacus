import unittest
from sqlalchemy_utils import database_exists, drop_database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from dateutil.parser import parse
from datetime import datetime
import importlib.util
from unittest import mock

from meerkat_abacus import data_management as manage
from meerkat_abacus import model, util, task_queue
from meerkat_abacus import config
spec = importlib.util.spec_from_file_location(
    "country_test",
    config.config_directory + config.country_config["country_tests"])
country_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(country_test)


class DbTest(unittest.TestCase):
    """
    Test setting up database functionality
    """

    def setUp(self):
        manage.create_db(config.DATABASE_URL, model.Base, drop=True)
        self.engine = create_engine(config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.conn = self.engine.connect()

    def tearDown(self):
        self.session.commit()
        self.session.close()
        self.engine.dispose()
        if database_exists(config.DATABASE_URL):
            drop_database(config.DATABASE_URL)

    def test_create_db(self):
        if database_exists(config.DATABASE_URL):    
            drop_database(config.DATABASE_URL)
        self.assertFalse(database_exists(config.DATABASE_URL))
        manage.create_db(config.DATABASE_URL, model.Base, drop=True)
        self.assertTrue(database_exists(config.DATABASE_URL))

    def test_locations(self):
        old_dir = manage.config.config_directory
        manage.config.config_directory = "meerkat_abacus/test/test_data/"
        old_locs = manage.country_config["locations"]
        manage.country_config["locations"] = {
            "clinics": "demo_clinics.csv",
            "districts": "demo_districts.csv",
            "regions": "demo_regions.csv"
        }
        manage.import_locations(self.engine, self.session)
        results = self.session.query(model.Locations).all()
        self.assertEqual(
            len(results),
            12)  # Important as we should not merge the final Clinic 1
        for r in results:
            #Checking that devideids are handled properly
            if r.name == "Clinic 1":
                if r.parent_location == 4:
                    self.assertEqual(r.deviceid, "1,6")
                else:
                    self.assertEqual(r.deviceid, "7")
            if r.name == "Clinic 2":
                self.assertEqual(r.start_date, datetime(2016, 2, 2))
            elif r.level == "clinic":
                self.assertEqual(
                    r.start_date,
                    manage.config.country_config["default_start_date"])
        # This file has a clinic with a non existent district
        old_clinic_file = manage.country_config["locations"]["clinics"]
        manage.country_config["locations"][
            "clinics"] = "demo_clinics_error.csv"
        with self.assertRaises(KeyError):
            manage.import_locations(self.engine, self.session)

        #Clean Up
        manage.country_config["locations"]["clinics"] = old_clinic_file
        manage.country_config["locations"] = old_locs
        manage.config.config_directory = old_dir

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
            )
        ]
        
        self.session.query(model.AggregationVariables).delete()
        self.session.commit()
        for v in variables:
            self.session.add(v)
        self.session.commit()
        
        manage.table_data_from_csv(
            "demo_case",
            model.form_tables["demo_case"],
            "meerkat_abacus/test/test_data/",
            self.session,
            self.engine,
            deviceids=["1", "2", "3", "4", "5", "6"],
            start_dates={"2": datetime(2016, 2, 2)},
            table_name="demo_case",
            quality_control=True)
        results = self.session.query(model.form_tables["demo_case"]).all()
        print(results)
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
            self.assertIn(r.uuid, ["3", "4", "5", "1"])
            
    @mock.patch('meerkat_abacus.util.requests')
    def test_db_setup(self, requests):
        task_queue.config.country_config["manual_test_data"] = {}
        task_queue.set_up_db.apply().get()
        self.assertTrue(database_exists(config.DATABASE_URL))
        engine = self.engine
        session = self.session
        #Locations
        results = session.query(model.Locations)
        country_test.test_locations(results)
        
        if config.fake_data:
            for table in model.form_tables:
                results = session.query(model.form_tables[table])
                #self.assertEqual(len(results.all()), 500)
        #Import variables
        agg_var = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.id == "tot_1").first()
        self.assertEqual(agg_var.name, "Total")

        # Number of cases

        n_cases = len(
            session.query(model.Data).filter(model.Data.type == "case").all())

        n_disregarded_cases =  len(
            session.query(model.DisregardedData).filter(model.Data.type == "case").all())
        
        t = model.form_tables[config.country_config["tables"][0]]
        n_expected_cases = len(
            session.query(t).filter(t.data["intro./visit"].astext == "new")
            .all())
        self.assertEqual(n_cases + n_disregarded_cases, n_expected_cases)

        agg_var_female = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.name == "Female").first()
        results = session.query(model.Data)
        results2 = session.query(model.DisregardedData)
        number_of_totals = 0
        number_of_female = 0
        for row in results:
            print(row)
            if "tot_1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female.id) in row.variables.keys():
                number_of_female += 1
        for row in results2:
            if "tot_1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female.id) in row.variables.keys():
                number_of_female += 1

                
        total = session.query(t).filter(
            t.data.contains({"intro./visit": "new"}))
        female = session.query(t).filter(
            t.data.contains({"intro./visit": "new",
                             agg_var_female.db_column: agg_var_female.condition
                             }))
        self.assertEqual(number_of_totals, len(total.all()))
        self.assertEqual(number_of_female, len(female.all()))
        session.close()
        self.assertFalse(manage.set_up_everything(True, False, 100))

    def test_get_proccess_data(self):
        old_fake = task_queue.config.fake_data
        old_s3 = task_queue.config.get_data_from_s3
        task_queue.config.fake_data = True
        task_queue.config.get_data_from_s3 = False
        task_queue.config.country_config["manual_test_data"] = {}
        manage.create_db(config.DATABASE_URL, model.Base, drop=True)

        numbers = {}
        manage.import_locations(self.engine, self.session)
        manage.import_variables(self.session)
        manage.add_fake_data(self.session, N=500, append=False)
        task_queue.get_proccess_data.apply().get()
        for table in model.form_tables:
            res = self.session.query(model.form_tables[table])
            numbers[table] = len(res.all())
        task_queue.get_proccess_data.apply().get()
        for table in model.form_tables:
            res = self.session.query(model.form_tables[table])
            self.assertEqual(numbers[table] + 5, len(res.all()))
        #Clean up
        task_queue.config.fake_data = old_fake
        task_queue.config.get_data_from_s3 = old_s3


if __name__ == "__main__":
    unittest.main()
