"""
Meerkat Abacus Test

Unit tests Meerkat Abacus
"""
from unittest import mock
import random
import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from meerkat_abacus import model
from meerkat_abacus.pipeline_worker.pipeline import Pipeline
from meerkat_abacus.consumer.database_setup import create_db
from meerkat_abacus.config import get_config
config = get_config()

class TestPipeline(unittest.TestCase):

    def setUp(self):
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.form_tables(config)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        pass

    def test_setup(self):
        param_config = get_config()
        param_config.country_config["pipeline"] = ["quality_control",
                                                   "write_to_db",
                                                   "quality_control"]
        engine = mock.MagicMock()
        session = mock.MagicMock()
        pipeline = Pipeline(engine, session, param_config)
        self.assertEqual(len(param_config.country_config["pipeline"]),
                         len(pipeline.pipeline))

    def test_process_chunk(self):
        param_config = get_config()
        param_config.country_config["pipeline"] = ["do_nothing",
                                                   "do_nothing",
                                                   "do_nothing"]

        engine = mock.MagicMock()
        session = mock.MagicMock()
        pipeline = Pipeline(engine, session, param_config)

        for i in range(30):
            data = []
            N = random.randint(10, 100)
            for _ in range(N):
                data.append({"form": "test-form",
                             "data": {"some-data": 4}})
            after_data = pipeline.process_chunk(data)
            self.assertEqual(data, after_data)
                           
    @mock.patch("meerkat_abacus.pipeline_worker.pipeline.DoNothing")
    def test_error_handling(self, do_nothing_mock):
        do_nothing_mock.return_value = mock.MagicMock(
            **{"run.side_effect": KeyError("Test Error")})
        param_config = get_config()
        param_config.country_config["pipeline"] = ["do_nothing"]
        pipeline = Pipeline(self.engine, self.session, param_config)

        data = []
        N = random.randint(1, 5)
        for _ in range(N):
            data.append({"form": "test-form",
                         "data": {"some-data": 4}})
        after_data = pipeline.process_chunk(data)
        self.assertEqual(after_data, [])
        
        results = self.session.query(model.StepFailiure).all()
        self.assertEqual(len(results), N)
        self.assertEqual(results[0].form, "test-form")
        self.assertEqual(results[0].error, "KeyError: 'Test Error'")

            
if __name__ == "__main__":
    unittest.main()
