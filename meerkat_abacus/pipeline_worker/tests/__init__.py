"""
Meerkat Abacus Test

Unit tests Meerkat Abacus
"""
from unittest.mock import MagicMock
import random
import unittest
from meerkat_abacus.pipeline_worker.pipeline import Pipeline
from meerkat_abacus import config


class TestPipeline(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_setup(self):
        param_config = config.get_config()
        param_config.country_config["pipeline"] = ["quality_control",
                                                   "write_to_db",
                                                   "quality_control"]
        engine = MagicMock()
        session = MagicMock()
        pipeline = Pipeline(engine, session, param_config)
        self.assertEqual(len(param_config.country_config["pipeline"]),
                         len(pipeline.pipeline))

    def test_process_chunk(self):
        param_config = config.get_config()
        param_config.country_config["pipeline"] = ["do_nothing",
                                                   "do_nothing",
                                                   "do_nothing"]

        engine = MagicMock()
        session = MagicMock()
        pipeline = Pipeline(engine, session, param_config)

        for i in range(30):
            data = []
            N = random.randint(10, 100)
            for _ in range(N):
                data.append({"form": "test-form",
                             "data": {"some-data": 4}})
            after_data = pipeline.process_chunk(data)
            self.assertEqual(data, after_data)
                           
        
if __name__ == "__main__":
    unittest.main()
