import unittest
from unittest import mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps.send_alerts import SendAlerts
from meerkat_abacus.consumer.database_setup import create_db
from meerkat_abacus.config import config

class TestSendAlerts(unittest.TestCase):

    def setUp(self):
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    @mock.patch('meerkat_abacus.pipeline_worker.process_steps.send_alerts.util.send_alert')
    def test_send_alert(self, send_alert_mock):
        send = SendAlerts(config, self.session)

        data = {"uuid": "abcdefghijk",
                "variables": {"alert": 1,
                              "alert_type": "individual"}
                }

        result = send.run("data", data)
        self.assertEqual(send_alert_mock.call_count, 1)
        self.assertEqual(result[0]["data"]["variables"]["alert_id"], "fghijk")
        
        data = {"uuid": "abcdefghijk",
                "variables": {}
                }

        result = send.run("data", data)
        self.assertEqual(send_alert_mock.call_count, 1)
        self.assertNotIn("alert_id", result[0]["data"]["variables"])
        data = {"uuid": "abcdefghijk",
                "variables": {"alert": 1,
                              "alert_type": "threshold"}
                }

        result = send.run("data", data)
        self.assertEqual(send_alert_mock.call_count, 1)
        self.assertNotIn("alert_id", result[0]["data"]["variables"])
        
