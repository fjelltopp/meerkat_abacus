import unittest
from sqlalchemy_utils import database_exists, drop_database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

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
        manage.fake_data(config.country_config,
                         config.form_directory, engine, N=500)
        results = session.query(manage.form_tables["case"])
        assert len(results.all()) == 500
        results = session.query(manage.form_tables["alert"])
        assert len(results.all()) == 500
        results = session.query(manage.form_tables["register"])
        assert len(results.all()) == 500

        manage.import_variables(config.country_config, engine)
        agg_var = session.query(model.AggregationVariables)
        assert agg_var.first().name == "Total"
        manage.raw_data_to_variables(engine)

        agg_var_female = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.name == "Female").first()
        results = session.query(model.Data)
        assert len(results.all()) == 1000
        number_of_totals = 0
        number_of_female = 0
        for row in results:
            if "1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female.id) in row.variables.keys():
                number_of_female += 1
        total = session.query(model.form_tables["case"]).filter(
            model.form_tables["case"].data.contains(
                {"intro./visit_type": 'new'}))
        female = session.query(model.form_tables["case"]).filter(
            model.form_tables["case"].data.contains(
                {"intro./visit_type": 'new', "pt1./gender": 'female'}))
        print(len(female.all()), number_of_female)
        assert number_of_totals == len(total.all())
        assert number_of_female == len(female.all())
