import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.process_steps import to_codes
from meerkat_abacus.consumer.database_setup import create_db
from meerkat_abacus.config import get_config
from geoalchemy2.shape import from_shape
from shapely.geometry import MultiPolygon


class TestToCode(unittest.TestCase):

    def setUp(self):
        config = get_config()
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def test_to_code(self):
        config = get_config()
        variables = [
            model.AggregationVariables(
                id="var_1", method="not_null", db_column="index", condition="",
                category=[],
                type="case",
                form="demo_case"),
            model.AggregationVariables(
                id="var_2",
                method="match",
                db_column="column1",
                alert=1,
                type="case",
                category=[],
                alert_type="individual",
                condition="A",
                form="demo_case"),
            model.AggregationVariables(
                id="var_3",
                category=[],
                type="case",
                method="sub_match",
                db_column="column2",
                condition="B",
                form="demo_case"),
            model.AggregationVariables(
                id="var_4",
                category=[],
                method="between",
                type="case",
                calculation="column3",
                db_column="column3",
                condition="5,10",
                disregard=1,
                form="demo_case"),
            model.AggregationVariables(
                id="var_confirmed",
                category=[],
                method="match",
                multiple_link="first",
                type="case",
                calculation="column3",
                db_column="confirmed",
                condition="yes",
                form="alert_investigation")
        ]
        for v in variables:
            self.session.add(v)
        self.session.commit()
        locations = {1: model.Locations(name="Demo", id=1),
             2: model.Locations(
                 name="Region 1", parent_location=1, id=2),
             3: model.Locations(
                 name="Region 2", parent_location=1, id=3),
             4: model.Locations(
                 name="District 1", parent_location=2,
                 level="district", id=4
             ),
             5: model.Locations(
                 name="District 2", parent_location=3,
                 level="district", id=5),
             6: model.Locations(
                 name="Clinic 1", parent_location=4,
                 deviceid="1",
                 id=6),
             7: model.Locations(
                 name="Clinic 2",
                 deviceid="2",
                 parent_location=5, id=7),
             8: model.Locations(
                 name="Clinic with no district", parent_location=2, id=8)}
        for l in locations.values():
            self.session.add(l)
        self.session.commit()

        data = {"type": "Case",
                "original_form": "demo_case",
                "raw_data": {
                    "SubmissionDate": "2017-01-14T05:38:33.482144",
                    "pt./visit_date": "2017-01-14T05:38:33.482144",
                    "meta/instanceID": "a1",
                    "index": 1,
                    "deviceid": "1",
                    "column1": "A",
                    "column2": "C",
                    "column3": "7"
                    }
                }
        tc = to_codes.ToCodes(config, self.session)
        result = tc.run("data", data)
        print(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["form"], "disregardedData")
        self.assertEqual(result[0]["data"]["date"],
                        datetime(2017, 1, 14))
        self.assertEqual(result[0]["data"]["clinic"], 6)
        self.assertIn("var_1", result[0]["data"]["variables"])
        self.assertIn("var_2", result[0]["data"]["variables"])
        self.assertNotIn("var_3", result[0]["data"]["variables"])
        self.assertIn("var_4", result[0]["data"]["variables"])

        data = {"type": "Case",
                "original_form": "demo_case",
                "raw_data": {
                    "SubmissionDate": "2017-01-14T05:38:33.482144",
                    "pt./visit_date": "2017-01-14T05:38:33.482144",
                    "meta/instanceID": "a1",
                    "index": 1,
                    "deviceid": "1",
                    "column1": "A",
                    "column2": "C",
                    "column3": "3"
                    },
                "link_data": {
                    "alert_investigation": [{
                        "confirmed": "yes",
                        "meta/instanceID": "b3"
                        }
                    ]
                }
                              
                }
        tc = to_codes.ToCodes(config, self.session)
        result = tc.run("data", data)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["form"], "data")
        self.assertEqual(result[0]["data"]["date"],
                        datetime(2017, 1, 14))
        self.assertIn("var_1", result[0]["data"]["variables"])
        self.assertIn("var_2", result[0]["data"]["variables"])
        self.assertNotIn("var_3", result[0]["data"]["variables"])
        self.assertNotIn("var_4", result[0]["data"]["variables"])
        self.assertIn("var_confirmed", result[0]["data"]["variables"])
