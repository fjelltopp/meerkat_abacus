import unittest
from datetime import datetime, timedelta
from dateutil import parser
import io

from meerkat_abacus import data_management as manage
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from meerkat_abacus import model

from meerkat_abacus import alerts
from meerkat_abacus import config


class UtilTest(unittest.TestCase):
    def setUp(self):
        manage.create_db(config.DATABASE_URL, model.Base, drop=True)
        self.engine = create_engine(config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        year = datetime.today().year
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

    def tearDown(self):
        pass

    def test_threshold(self):

        self.session.query(model.Data).delete()
        self.session.commit()
        self.session.bulk_save_objects(self.threshold)
        self.session.commit()

        new_alerts = alerts.threshold("cmd_1", [3, 5], self.session)
        self.assertEqual(len(new_alerts), 2)

        self.assertEqual(new_alerts[0]["duration"], 1)
        self.assertEqual(sorted(new_alerts[0]["uuids"]), ["1", "2", "3"])
        self.assertEqual(new_alerts[0]["clinic"], 6)
        self.assertEqual(new_alerts[0]["reason"], "cmd_1")
        self.assertEqual(new_alerts[0]["date"], datetime(self.year, 1, 3))
        
        self.assertEqual(new_alerts[1]["duration"], 7)
        self.assertEqual(sorted(new_alerts[1]["uuids"]),
                         ["4", "5", "6", "7", "8"])
        self.assertEqual(new_alerts[1]["clinic"], 6)
        self.assertEqual(new_alerts[1]["reason"], "cmd_1")
        self.assertEqual(new_alerts[1]["date"], datetime(self.year, 1, 8))
