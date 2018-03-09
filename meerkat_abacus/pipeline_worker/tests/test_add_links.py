import unittest
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

from datetime import datetime
from meerkat_abacus.config import get_config
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps import add_links
from meerkat_abacus.consumer.database_setup import create_db
config = get_config()


class TestAddLinks(unittest.TestCase):

    def setUp(self):
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        con = self.engine.connect()
        table = model.form_tables(config)["demo_case"]
        con.execute(table.__table__.delete())
        table = model.form_tables(config)["demo_alert"]
        con.execute(table.__table__.delete())
        
    test_links = (
        {"Case": [{
            "name": "alert_investigation",
            "to_form": "demo_alert",
            "from_form": "demo_case",
            "from_column": "alert_id",
            "to_column": "alert_id",
            "method": "match",
            "order_by": "visit_data;date",
            "uuid": "meta/instanceID"
            }]
         },
        {"alert_investigation": {
            "name": "alert_investigation",
            "to_form": "demo_alert",
            "from_form": "demo_case",
            "from_column": "alert_id",
            "to_column": "alert_id",
            "method": "match",
            "order_by": "visit_data;date",
            "uuid": "meta/instanceID"
            }})
    
    @patch.object(add_links.util, 'get_links',
                  return_value=test_links)
    def test_add_to_links(self, get_links_mock):
        config = get_config()
        al = add_links.AddLinks(config, self.session)
        existing_data = [{
            "uuid": "a",
            "data": {
                "visit_date": "2017-01-14T05:38:33.482144",
                "icd_code": "A01",
                "patientid": "1",
                "alert_id": "a1",
                "module": "ncd",
                "intro./visit": "new",
                "id": "1"
            }
        },
        {
            "uuid": "b",
            "data": {
                "visit_date": "2017-01-14T05:38:33.482144",
                "icd_code": "A01",
                "patientid": "1",
                "alert_id": "a2",
                "module": "ncd",
                "intro./visit": "new",
                "id": "2"
            }
        }
        ]
        table = model.form_tables(config)["demo_case"]
        con = self.engine.connect()
        con.execute(table.__table__.insert(), existing_data)
        con.close()
        test_data = {"type": "Case",
                     "original_form": "demo_alert",
                     "link_data": {"alert_investigation": [{"alert_id": "a1"}]}
                     }
        results = al.run("data", test_data)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["data"]["raw_data"],
                         existing_data[0]["data"])
        self.assertEqual(results[0]["data"]["link_data"],
                         test_data["link_data"])

    test_links2 = (
        {"Case": [{
            "name": "alert_investigation",
            "to_form": "demo_alert",
            "from_form": "demo_case",
            "from_column": "alert_id",
            "to_column": "alert_id",
            "method": "match",
            "order_by": "visit_data;date",
            "uuid": "meta/instanceID"
            }]
         },
         {"alert_investigation": {
            "name": "alert_investigation",
            "to_form": "demo_alert",
            "from_form": "demo_case",
            "from_column": "alert_id",
            "to_column": "alert_id",
            "method": "match",
            "order_by": "visit_data;date",
            "uuid": "meta/instanceID"
            }})

    @patch.object(add_links.util, 'get_links',
                  return_value=test_links2)
    def test_add_from_links(self, get_links_mock):
        config = get_config()
        config.country_config["alert_id_length"] = 1
        al = add_links.AddLinks(config, self.session)
        existing_data = [{
            "uuid": "a",
            "data": {
                "alert_id": "1",
            }
        }
        ]
        table = model.form_tables(config)["demo_alert"]
        con = self.engine.connect()
        con.execute(table.__table__.insert(), existing_data)
        con.close()
        test_data = {"type": "Case",
                     "original_form": "demo_case",
                     "raw_data": {"alert_id": "1",
                                  "intro./visit": "new"}
                     }
        results = al.run("data", test_data)
        logging.info(results)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["data"]["raw_data"],
                         test_data["raw_data"])
        self.assertEqual(results[0]["data"]["link_data"],
                         {"alert_investigation": [existing_data[0]["data"]]})

    test_links3 = (
        {"Case": [{
            "name": "return_visit",
            "to_form": "demo_case",
            "from_form": "demo_case",
            "from_column": "link_id",
            "to_column": "link_id",
            "method": "lower_match",
            "order_by": "visit_date;date",
            "uuid": "meta/instanceID",
            "to_condition": "visit:return"
            }]
         },
        {"return_visit": {
            "name": "return_visit",
            "to_form": "demo_case",
            "from_form": "demo_case",
            "from_column": "link_id",
            "to_column": "link_id",
            "method": "lower_match",
            "order_by": "visit_date;date",
            "uuid": "meta/instanceID",
            "to_condition": "visit:return"
            }})

    @patch.object(add_links.util, 'get_links',
                  return_value=test_links3)
    def test_self_link_lower_match(self, get_links_mock):
        config = get_config()
        config.country_config["alert_id_length"] = 1
        al = add_links.AddLinks(config, self.session)
        existing_data = [{
            "uuid": "a",
            "data": {
                "visit_date": "2017-01-14T05:38:33.482144",
                "icd_code": "A01",
                "patientid": "1",
                "alert_id": "aa",
                "module": "ncd",
                "intro./visit": "new",
                "id": "1"
            }
        },
        {
            "uuid": "b",
            "data": {
                "visit_date": "2017-01-17T05:38:33.482144",
                "link_id": "AA",
                "visit": "return",
                "id": "2"
            }
        }
        ]
        table = model.form_tables(config)["demo_case"]
        con = self.engine.connect()
        con.execute(table.__table__.insert(), existing_data)
        con.close()
        test_data = {"type": "Case",
                     "original_form": "demo_case",
                     "link_data": {"return_visit": [{
                         "link_id": "Aa",
                         "visit": "return",
                         "id": "3",
                         "visit_date": "2017-01-16T05:38:33.482144"}]
                     }
        }
        results = al.run("data", test_data)
        logging.info(results)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["data"]["link_data"]["return_visit"]),
                         2)
        # Make sure they are in right order
        self.assertEqual(results[0]["data"]["link_data"]["return_visit"][0]["id"],
                         "3")
        self.assertEqual(results[0]["data"]["link_data"]["return_visit"][1]["id"],
                         "2")
