import unittest

from meerkat_abacus.config import get_config
from meerkat_abacus.pipeline_worker.process_steps import to_data_type


class TestToDataType(unittest.TestCase):

    def setUp(self):
        pass

    def test_to_data_type(self):
        config = get_config()

        tdt = to_data_type.ToDataType(config)
        
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

