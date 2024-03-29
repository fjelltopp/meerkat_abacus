import unittest
from unittest import mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps import add_multiple_alerts
from meerkat_abacus.pipeline_worker.process_steps import write_to_db
from meerkat_abacus.consumer.database_setup import create_db

from meerkat_abacus.config import config

class TestAddMultipleAlerts(unittest.TestCase):

    def setUp(self):
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def test_app_multiple_alerts(self):

        existing_raw_data = [
            {
                "uuid": "a",
                "data": {
                    "SubmissionDate": "2017-06-10",
                    "end": "2017-06-10",
                    "pt1./gender": "male",
                    "pt1./age": 32
                }
            },
            {
                "uuid": "b",
                "data": {
                    "SubmissionDate": "2017-06-10",
                    "end": "2017-06-10",
                    "pt1./gender": "male",
                    "pt1./age": 32
                }
            },
            {
                "uuid": "c",
                "data": {
                    "SubmissionDate": "2017-06-10",
                    "end": "2017-06-10",
                    "pt1./gender": "male",
                    "pt1./age": 32
                }
            }
        ]
        existing_data = [
            {
                "clinic": 1,
                "uuid": "a",
                "type": "case",
                "date": datetime(2017, 6, 10),
                "variables": {
                    "cmd_1": 1
                }
            },
            {
                "clinic": 1,
                "uuid": "b",
                "type": "case",
                "date": datetime(2017, 6, 10),
                "variables": {
                    "cmd_1": 1
                }
            },
            {
                "clinic": 1,
                "uuid": "c",
                "type": "case",
                "date": datetime(2017, 6, 10),
                "variables": {
                    "cmd_1": 1
                }
            }
         
        ]
        table = model.form_tables(config)["demo_case"]
        con = self.engine.connect()
        con.execute(table.__table__.insert(), existing_raw_data)
        con.execute(model.Data.__table__.insert(), existing_data)

        variable = model.AggregationVariables(
            id="cmd_1",
            method="match", db_column="icd_code",
            type="case",
            condition="A00",
            category=[],
            alert=1,
            alert_type="threshold:3,5",
            form="demo_case")
        self.session.add(variable)
        self.session.commit()
        add_alerts = add_multiple_alerts.AddMultipleAlerts(config,
                                                           self.session)
        new_data = {
                "clinic": 1,
                "uuid": "c",
                "date": datetime(2017, 6, 10),
                "variables": {
                    "cmd_1": 1
                }
            }
        add_alerts.start_step()
        results = []
        for d in existing_data:
            results += add_alerts.run("data", d)
        add_alerts.end_step(3)
        self.assertEqual(len(results), 3)
        self.assertEqual(len(add_alerts.found_uuids), 3)
        for result in results:
            if result["data"]["uuid"] == "a":
                self.assertIn("alert", result["data"]["variables"])
                self.assertIn("alert_id", result["data"]["variables"])
            else:
                self.assertNotIn("alert", result["data"]["variables"])
                self.assertNotIn("alert_id", result["data"]["variables"])
                self.assertEqual(result["data"]["variables"]["sub_alert"], 1)
                self.assertEqual(result["data"]["variables"]["master_alert"],
                                 "a")

        db_writer = write_to_db.WriteToDb(config, self.session)
        db_writer.engine = self.engine
        db_writer.start_step()
        for r in results:
            db_writer.run(r["form"], r["data"])
        db_writer.end_step(3)
        
        additional_data = [
            {
                "clinic": 1,
                "uuid": "d",
                "type": "case",
                "date": datetime(2017, 6, 10),
                "variables": {
                    "cmd_1": 1
                }
            }
        ]
        additional_raw_data = [
            {
                "uuid": "d",
                "data": {
                    "SubmissionDate": "2017-06-10",
                    "end": "2017-06-10",
                    "pt1./gender": "male",
                    "pt1./age": 32
                }
            }
        ]
        con.execute(table.__table__.insert(), additional_raw_data)
        con.execute(model.Data.__table__.insert(), additional_data)

        add_alerts.start_step()
        results = add_alerts.run("data", new_data)
        self.assertEqual(len(results), 4)
        for result in results:
            if result["data"]["uuid"] == "a":
                self.assertIn("alert", result["data"]["variables"])
                self.assertIn("alert_id", result["data"]["variables"])
            else:
                self.assertNotIn("alert", result["data"]["variables"])
                self.assertNotIn("alert_id", result["data"]["variables"])
                self.assertEqual(result["data"]["variables"]["sub_alert"], 1)
                self.assertEqual(result["data"]["variables"]["master_alert"],
                                 "a")

                

class TestAlertTypes(unittest.TestCase):
    def setUp(self):
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        year = 2014
        self.year = year
        self.threshold = [
            model.Data(
                date=datetime(year, 1, 3),
                clinic=6,
                uuid="1",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 3),
                clinic=6,
                uuid="2",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 3),
                clinic=6,
                uuid="3",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                clinic=6,
                uuid="4",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                clinic=6,
                uuid="5",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 11),
                clinic=6,
                uuid="6",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 11),
                clinic=6,
                uuid="7",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 12),
                clinic=6,
                uuid="8",
                variables={"cmd_1": 1}),
        ]
        self.double = [
            model.Data(
                date=datetime(year, 1, 3),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="1",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 3),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="2",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="3",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="4",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="5",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="6",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="7",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="8",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="9",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="10",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="11",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="12",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="13",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="14",
                variables={"cmd_1": 1}),
 
        ]
        self.double_wrong_clinic = [
            model.Data(
                date=datetime(year, 1, 3),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="1",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 3),
                epi_year=year,
                epi_week=1,
                clinic=7,
                uuid="2",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="3",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="4",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="5",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="6",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="7",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="8",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="9",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="10",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="11",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="12",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="13",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=3,
                clinic=6,
                uuid="14",
                variables={"cmd_1": 1}),
 
        ]
        self.double_new_year = [
            model.Data(
                date=datetime(year, 1, 3),
                epi_year=year - 1,
                epi_week=52,
                clinic=6,
                uuid="1",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 3),
                epi_year=year - 1,
                epi_week=52,
                clinic=6,
                uuid="2",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="3",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="4",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="5",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
                epi_year=year,
                epi_week=1,
                clinic=6,
                uuid="6",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="7",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="8",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="9",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="10",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="11",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="12",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="13",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                epi_year=year,
                epi_week=2,
                clinic=6,
                uuid="14",
                variables={"cmd_1": 1}),
 
        ]
        self.session.commit()

    def tearDown(self):
        self.session.commit()
        self.session.close()

    def test_threshold(self):

        self.session.query(model.Data).delete()
        self.session.commit()
        self.session.bulk_save_objects(self.threshold)
        self.session.commit()

        new_alerts = add_multiple_alerts.threshold("cmd_1",
                                                   "threshold:3,5",
                                                   datetime(self.year, 1, 3),
                                                   6,
                                                   self.session)

        self.assertEqual(len(new_alerts), 1)

        self.assertEqual(new_alerts[0]["duration"], 1)
        self.assertEqual(sorted(new_alerts[0]["uuids"]), ["1", "2", "3"])
        self.assertEqual(new_alerts[0]["clinic"], 6)
        self.assertEqual(new_alerts[0]["reason"], "cmd_1")
        new_alerts = add_multiple_alerts.threshold("cmd_1",
                                                   "threshold:3,5",
                                                   datetime(self.year, 1, 11),
                                                   6,
                                                   self.session)
        self.assertEqual(len(new_alerts), 1)
        self.assertEqual(new_alerts[0]["duration"], 7)
        self.assertEqual(
            sorted(new_alerts[0]["uuids"]), ["4", "5", "6", "7", "8"])
        self.assertEqual(new_alerts[0]["clinic"], 6)
        self.assertEqual(new_alerts[0]["reason"], "cmd_1")

    def test_double_double(self):
        self.session.query(model.Data).delete()
        self.session.commit()
        self.session.bulk_save_objects(self.double_wrong_clinic)
        self.session.commit()

        new_alerts = add_multiple_alerts.double_double("cmd_1",
                                                       1,
                                                       self.year,
                                                       6,
                                                       self.engine)
        self.assertEqual(len(new_alerts), 0)

        
        self.session.query(model.Data).delete()
        self.session.commit()
        self.session.bulk_save_objects(self.double)
        self.session.commit()

        new_alerts = add_multiple_alerts.double_double("cmd_1",
                                                       1,
                                                       self.year,
                                                       6,
                                                       self.engine)
        self.assertEqual(len(new_alerts), 1)

        self.assertEqual(new_alerts[0]["duration"], 7)
        self.assertEqual(
            sorted(new_alerts[0]["uuids"]),
            sorted(["7", "8", "9", "10", "11", "12", "13", "14"]))
        self.assertEqual(new_alerts[0]["clinic"], 6)
        self.assertEqual(new_alerts[0]["reason"], "cmd_1")
        # sert False
        to_delete = self.session.query(model.Data).filter(model.Data.uuid == "1").one()
        self.session.delete(to_delete)
        self.session.commit()

        # With only one entry for the first week, we should not create an alert
        new_alerts = add_multiple_alerts.double_double("cmd_1",
                                                       1,
                                                       self.year,
                                                       6,
                                                       self.engine)
        self.assertEqual(len(new_alerts), 0)

    def test_double_double_corner_cases(self):
        self.session.query(model.Data).delete()
        self.session.commit()
        self.session.bulk_save_objects(self.double_new_year)
        self.session.commit()

        for week in [52, 1, 2]:
            year = self.year
            if week == 52:
                year = year - 1
            new_alerts = add_multiple_alerts.double_double("cmd_1",
                                                           week,
                                                           year,
                                                           6,
                                                           self.engine)
            self.assertEqual(len(new_alerts), 1)
            
            self.assertEqual(new_alerts[0]["duration"], 7)
            self.assertEqual(
                sorted(new_alerts[0]["uuids"]),
                sorted(["7", "8", "9", "10", "11", "12", "13", "14"]))
            self.assertEqual(new_alerts[0]["clinic"], 6)
            self.assertEqual(new_alerts[0]["reason"], "cmd_1")
        
        

if __name__ == "__main__":
    unittest.main()
