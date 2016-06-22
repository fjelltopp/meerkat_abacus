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
        manage.create_db(config.DATABASE_URL,
                         model.Base,
                         drop=True)
        self.engine = create_engine(config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def tearDown(self):
        self.session.commit()
        self.session.close()
        if database_exists(config.DATABASE_URL):
            drop_database(config.DATABASE_URL)
        
    def test_create_db(self):
        if database_exists(config.DATABASE_URL):
            drop_database(config.DATABASE_URL)
        self.assertFalse(database_exists(config.DATABASE_URL))
        manage.create_db(config.DATABASE_URL,
                         model.Base,
                         drop=True)
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
        self.assertEqual(len(results), 12)  # Important as we should not merge the final Clinic 1
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
                self.assertEqual(r.start_date,
                                 manage.config.country_config["default_start_date"])
        # This file has a clinic with a non existent district
        old_clinic_file = manage.country_config["locations"]["clinics"]
        manage.country_config["locations"]["clinics"] = "demo_clinics_error.csv"
        with self.assertRaises(KeyError):
            manage.import_locations(self.engine, self.session)


        #Clean Up
        manage.country_config["locations"]["clinics"] = old_clinic_file
        manage.country_config["locations"] = old_locs
        manage.config.config_directory = old_dir

    def test_should_row_be_added(self):
        """ testing should row be added"""
        row = {"deviceid": "2", "pt./visit_date": "10/01/2016"}

        self.assertTrue(manage.should_row_be_added(row,
                                                   "case",
                                                   ["2"],
                                                   {"2": datetime(2016, 1, 1)}
                                                   )
                        )
        self.assertTrue(manage.should_row_be_added(row,
                                                   "case",
                                                   None,
                                                   {"2": datetime(2016, 1, 1)}
                                                   )
                        )
        self.assertTrue(manage.should_row_be_added(row,
                                                   "case",
                                                   ["2"],
                                                   None,
                                                   )
                        )
        self.assertFalse(manage.should_row_be_added(row,
                                                   "case",
                                                   ["3"],
                                                   {"2": datetime(2016, 1, 1)}
                                                   )
                        )
        self.assertFalse(manage.should_row_be_added(row,
                                                   "case",
                                                   ["2"],
                                                   {"2": datetime(2016, 1, 11)}
                                                   )
                        )
        
                                                    
        
    def test_table_data_from_csv(self):
        """Test table_data_from_csv"""
        
        manage.table_data_from_csv("demo_case", model.form_tables["case"],
                                   "meerkat_abacus/test/test_data/",
                                   self.session, self.engine,
                                   deviceids=["1", "2", "3", "4", "5", "6"],
                                   start_dates={"2": datetime(2016, 2, 2)},
                                   table_name="case")
        results = self.session.query(model.form_tables["case"]).all()
        self.assertEqual(len(results), 5)  # Only 6 of the cases have deviceids in 1-6 and one case has a too early date
        for r in results:
            self.assertIn(r.uuid, ["1",  "3", "4", "5", "6"])

    def test_links(self):
        deviceids = ["1", "2", "3", "4", "5", "6", "7", "8"]

        manage.create_db(config.DATABASE_URL,
                         model.Base,
                         drop=True)

        manage.table_data_from_csv("demo_case", model.form_tables["case"],
                                   "meerkat_abacus/test/test_data/",
                                   self.session, self.engine,
                                   table_name=config.country_config["tables"]["case"],
                                   deviceids=deviceids)
        manage.table_data_from_csv("demo_alert", model.form_tables["alert"],
                                   "meerkat_abacus/test/test_data/",
                                   self.session, self.engine,
                                   deviceids=deviceids,
                                   table_name=config.country_config["tables"]["alert"])
        link_def = {
            "id": "test",
            "name": "Test",
            "from_table": "form_tables.case",
            "from_column": "meta/instanceID",
            "from_condition": "intro./visit:new",
            "from_date": "start",
            "to_table": "alert",
            "to_column": "link_to",
            "to_date": "end",
            "to_condition": "condition:yes",
            "which": "last",
            "data": {
                "status": {
                    "A": {"column": "letter",
                          "condition": "A"},
                    "B": {"column": "letter",
                          "condition": "B"},
                    "C": {"column": ["index", "letter"],
                          "condition": "7"}
                }
            }
        }
        old_links = manage.config.links.links
        manage.config.links.links = [link_def]
        manage.import_links(self.session)
        self.session.query(model.Links).delete()
        self.session.commit()

        manage.add_new_links()

        links = self.session.query(model.Links).all()
        # Should be 5 links 
        self.assertEqual(5, len(links))
        for link in links:
            if link.link_value == "1":
                # Check that it got the latest
                self.assertEqual(link.data, {"status": "B"}) 
            if link.link_value == "8":
                self.assertEqual(link.data, {"status": "C"}) 
        #Clean up
        manage.config.links.links = old_links
        
    @mock.patch('meerkat_abacus.util.requests')
    def test_db_setup(self, requests):
        
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
                self.assertEqual(len(results.all()), 500)
        #Import variables
        agg_var = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.id == "tot_1").first()
        self.assertEqual(agg_var.name, "Total")
        
        link_defs = session.query(model.LinkDefinitions)
        self.assertEqual(link_defs.first().name, "Alert Investigation")
        #To codes
        agg_var_female = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.name == "Female").first()
        results = session.query(model.Data)
        sec_condition = agg_var.secondary_condition.split(":")
        number_of_totals = 0
        number_of_female = 0
        for row in results:
            if "tot_1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female.id) in row.variables.keys():
                number_of_female += 1
        total = session.query(model.form_tables["case"]).filter(
            model.form_tables["case"].data.contains(
                {sec_condition[0]: sec_condition[1]}))
        female = session.query(model.form_tables["case"]).filter(
            model.form_tables["case"].data.contains(
                {sec_condition[0]: sec_condition[1],
                 agg_var_female.db_column: agg_var_female.condition}))
        self.assertEqual(number_of_totals, len(total.all()))
        self.assertEqual(number_of_female, len(female.all()))
        #Add links
        manage.add_new_links()
        link_query = session.query(model.Links).filter(
            model.Links.link_def == "alert_investigation")
        links = {}
        for link in link_query:
            links[link.link_value] = link
        
        alert_query = session.query(model.Alerts)
        alerts = {}
        for a in alert_query:
            alerts[a.id] = a
        alert_inv_query = session.query(model.form_tables["alert"])
        alert_invs = {}
        for a in alert_inv_query:
            alert_invs.setdefault(a.data["pt./alert_id"], [])
            alert_invs[a.data["pt./alert_id"]].append(a)

        for alert_id in alerts.keys():
            if alert_id in alert_invs.keys():
                self.assertIn(alert_id, links.keys())
                if len(alert_invs[alert_id]) == 1:
                    self.assertEqual(links[alert_id].to_date,
                                     parse(alert_invs[alert_id][0].data["end"]))
                    labs = (alert_invs[alert_id][0]
                            .data["alert_labs./return_lab"])
                    country_test.test_alert_status(labs, links[alert_id])
      
                else:
                    investigations = alert_invs[alert_id]
                    largest_date = datetime(2015, 1, 1)
                    largest_inv = None
                    for inv in investigations:
                        if parse(inv.data["end"]) > largest_date:
                            largest_date = parse(inv.data["end"])
                            largest_inv = inv
                    self.assertEqual(links[alert_id].to_date, largest_date)
                    labs = (largest_inv
                            .data["alert_labs./return_lab"])
                    country_test.test_alert_status(labs, links[alert_id])
            else:
                self.assertNotIn(alert_id, links.keys())
        session.close()

        self.assertFalse(
            manage.set_up_everything(True, False, 100)
            )

    def test_get_proccess_data(self):
        old_fake = task_queue.config.fake_data
        old_s3 = task_queue.config.get_data_from_s3
        task_queue.config.fake_data = True
        task_queue.config.get_data_from_s3 = False
        manage.create_db(config.DATABASE_URL,
                         model.Base,
                         drop=True)
        
        numbers = {}
        manage.import_locations(self.engine, self.session)
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
