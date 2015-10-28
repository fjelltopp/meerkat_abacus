import unittest
from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from meerkat_abacus import manage
from meerkat_abacus import model
from meerkat_abacus import config


class DbTest(unittest.TestCase):
    """
    Test setting up database functionality
    """
    def setUp(self):
        pass

    def tearDown(self):
        if database_exists(config.DATABASE_URL):
            drop_database(config.DATABASE_URL)

    def test_db_setup(self):
        manage.create_db(config.DATABASE_URL,
                         model.Base,
                         config.country_config,
                         drop=True)
        assert database_exists(config.DATABASE_URL)
        engine = create_engine(config.DATABASE_URL)
        manage.import_locations(config.country_config, engine)
        Session = sessionmaker(bind=engine)

        session = Session()
        results = session.query(model.Locations)
        assert len(results.all()) == 11
        for r in results:
            if r.id == 1:
                assert r.name == "Demo"
            if r.id == 5:
                assert r.name == "District 2"
                assert r.parent_location == 2
            if r.id == 7:
                assert r.deviceid == "1,6"
        manage.import_data(config.country_config,
                           config.form_directory,
                           engine)
        results = session.query(manage.form_tables["case"])
        assert len(results.all()) == 500
        results = session.query(manage.form_tables["alert"])
        assert len(results.all()) == 500
        results = session.query(manage.form_tables["register"])
        assert len(results.all()) == 500

        manage.import_variables(config.country_config, engine)
        results = session.query(model.AggregationVariables).first()
        assert results.name == "Total"
