import unittest
from datetime import datetime, timedelta
from dateutil import parser
import io

from meerkat_abacus import data_management as manage
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from meerkat_abacus import model

from meerkat_abacus import alerts
from meerkat_abacus.config import config


class UtilTest(unittest.TestCase):
    def setUp(self):
        manage.create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
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
                clinic=6,
                uuid="1",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 3),
                clinic=6,
                uuid="2",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 10),
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
                date=datetime(year, 1, 10),
                clinic=6,
                uuid="6",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="7",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="8",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="9",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="10",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="11",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="12",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="13",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 1, 17),
                clinic=6,
                uuid="14",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 10),
                clinic=6,
                uuid="15",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 10),
                clinic=6,
                uuid="16",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 10),
                clinic=6,
                uuid="17",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 10),
                clinic=6,
                uuid="18",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="19",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="20",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="21",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="22",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="23",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="24",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="25",
                variables={"cmd_1": 1}),
            model.Data(
                date=datetime(year, 2, 17),
                clinic=6,
                uuid="26",
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

        new_alerts = alerts.threshold("cmd_1", [3, 5], self.session)
        print(new_alerts)

        self.assertEqual(len(new_alerts), 2)

        self.assertEqual(new_alerts[0]["duration"], 1)
        self.assertEqual(sorted(new_alerts[0]["uuids"]), ["1", "2", "3"])
        self.assertEqual(new_alerts[0]["clinic"], 6)
        self.assertEqual(new_alerts[0]["reason"], "cmd_1")

        self.assertEqual(new_alerts[1]["duration"], 7)
        self.assertEqual(
            sorted(new_alerts[1]["uuids"]), ["4", "5", "6", "7", "8"])
        self.assertEqual(new_alerts[1]["clinic"], 6)
        self.assertEqual(new_alerts[1]["reason"], "cmd_1")

    def test_double_double(self):

        self.session.query(model.Data).delete()
        self.session.commit()
        self.session.bulk_save_objects(self.double)
        self.session.commit()

        new_alerts = alerts.double_double("cmd_1", self.session)
        self.assertEqual(len(new_alerts), 1)

        self.assertEqual(new_alerts[0]["duration"], 7)
        self.assertEqual(
            sorted(new_alerts[0]["uuids"]),
            sorted(["7", "8", "9", "10", "11", "12", "13", "14"]))
        self.assertEqual(new_alerts[0]["clinic"], 6)
        self.assertEqual(new_alerts[0]["reason"], "cmd_1")


    def test_double_double_id(self):
        self.session.query(model.Data).delete()
        self.session.commit()
        config.country_config["alert_data"] = {"demo_case": {}}
        self.session.bulk_save_objects(self.double)
        self.session.commit()
        self.session.query(model.AggregationVariables).delete()
        self.session.add(
            model.AggregationVariables(
                id="cmd_1",
                alert=1,
                alert_type="double",
                form="demo_case"
                )
            )
        for d in self.double:
            self.session.add(
                model.form_tables()["demo_case"](
                    uuid=d.uuid,
                    )
                )
        self.session.commit()
        n_alerts = 0
        manage.add_alerts(self.session, config)
        for d in self.session.query(model.Data):
            print(d.variables)
            if "alert" in d.variables:
                n_alerts += 1
        self.assertEqual(n_alerts, 1)

        for i in range(5):
            manage.add_alerts(self.session, config)
        n_alerts = 0
        for d in self.session.query(model.Data):
            if "alert" in d.variables:
                n_alerts += 1
        self.assertEqual(n_alerts, 1)
