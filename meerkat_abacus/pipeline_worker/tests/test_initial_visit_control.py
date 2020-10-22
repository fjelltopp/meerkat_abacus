import unittest
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime
from meerkat_abacus.config import config
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps import initial_visit_control
from meerkat_abacus.consumer.database_setup import create_db


class TestInitialVisitControl(unittest.TestCase):

    def setUp(self):

        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def test_initial_visit_control(self):
        config.country_config["initial_visit_control"] = {
            "demo_case": {
                "identifier_key_list": ["patientid", "icd_code"],
                "visit_type_key": "intro./visit",
                "visit_date_key": "visit_date",
                "module_key": "module",
                "module_value": "ncd"

            }
        }

        ivc = initial_visit_control.InitialVisitControl(config, self.session)
        
        existing_data = [{
            "uuid": "a",
            "data": {
                "visit_date": "2017-01-14T05:38:33.482144",
                "icd_code": "A01",
                "patientid": "1",
                "module": "ncd",
                "intro./visit": "new",
                "id": "1"
            }
        }]
        table = model.form_tables(config)["demo_case"]
        con = self.engine.connect()
        con.execute(table.__table__.insert(), existing_data)
        con.close()

        new_data = {
            "visit_date": "2017-02-14T05:38:33.482144",
            "icd_code": "A01",
            "patientid": "1",
            "module": "ncd",
            "intro./visit": "new",
            "id": "2"
        }

        new_data_wrong_module = {
            "visit_date": "2017-02-14T05:38:33.482144",
            "icd_code": "A01",
            "patientid": "1",
            "module": "cd",
            "intro./visit": "new"
        }

        new_data_different_pid = {
            "visit_date": "2017-02-14T05:38:33.482144",
            "icd_code": "A01",
            "patientid": "2",
            "module": "ncd",
            "intro./visit": "new"
        }

        new_data_different_icd = {
            "visit_date": "2017-02-14T05:38:33.482144",
            "icd_code": "A02",
            "patientid": "1",
            "module": "ncd",
            "intro./visit": "new"
        }

        result = ivc.run("demo_register", new_data)[0]
        self.assertEqual(result["form"], "demo_register")
        self.assertEqual(result["data"], new_data)

        result = ivc.run("demo_case", new_data_wrong_module)
        self.assertEqual(len(result), 1)
        result = ivc.run("demo_case", new_data_different_pid)
        self.assertEqual(len(result), 1)
        result = ivc.run("demo_case", new_data_different_icd)
        self.assertEqual(len(result), 1)
        
        result = ivc.run("demo_case", new_data)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1]["data"]["intro./visit"], "return")
        self.assertEqual(result[1]["data"]["id"], "2")

        new_data = {
            "visit_date": "2017-01-01T05:38:33.482144",
            "icd_code": "A01",
            "patientid": "1",
            "module": "ncd",
            "intro./visit": "new",
            "id": "2"
        }
        result = ivc.run("demo_case", new_data)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1]["data"]["intro./visit"], "return")
        self.assertEqual(result[1]["data"]["id"], "1")
