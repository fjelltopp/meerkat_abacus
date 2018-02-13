import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from meerkat_abacus.config import get_config
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps import to_data_type
from meerkat_abacus.consumer.database_setup import create_db

class TestToDataType(unittest.TestCase):

    def setUp(self):
        config = get_config()
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def test_to_data_type(self):
        config = get_config()

        tdt = to_data_type.ToDataType(config, self.session)
        
        data_1 = {"form": "demo_case",
                  "data": {"intro./visit": "new"}}
        data_2 = {"form": "demo_case",
                  "data": {"intro./visit": "return"}}
        data_3 = {"form": "demo_alert",
                  "data": {"intro./visit": "new"}}
        data_4 = {"form": "demo_register",
                  "data": {"intro./visit": "new"}}
        data_5 = {"form": "demo_does_not_exisit",
                  "data": {"intro./visit": "new"}}

        result = tdt.run(data_1["form"], data_1["data"])
        self.assertEqual(len(result), 2)
        types = [d["data"]["type"] for d in result]
        self.assertEqual(["Case", "Visit"], sorted(types))

        self.assertEqual(result[0]["data"].get("raw_data"), data_1["data"])
        
        result = tdt.run(data_2["form"], data_2["data"])
        self.assertEqual(len(result), 1)
        types = [d["data"]["type"] for d in result]
        self.assertEqual(["Visit"], sorted(types))
        result = tdt.run(data_3["form"], data_3["data"])
        self.assertEqual(len(result), 1)
        types = [d["data"]["type"] for d in result]
        self.assertEqual(["Case"], sorted(types))
        self.assertEqual(result[0]["data"].get("link_data"),
                         {"alert_investigation": [data_1["data"]]})

        result = tdt.run(data_4["form"], data_4["data"])
        self.assertEqual(len(result), 1)
        types = [d["data"]["type"] for d in result]
        self.assertEqual(["Register"], sorted(types))

        result = tdt.run(data_5["form"], data_5["data"])
        self.assertEqual(result, [])

