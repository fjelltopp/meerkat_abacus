import unittest
from sqlalchemy_utils import database_exists, drop_database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from dateutil.parser import parse
from datetime import datetime

from meerkat_abacus import data_management as manage
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
        form_directory = config.data_directory + "forms/"
        manage.fake_data(config.country_config,
                         form_directory, engine, N=500)
        manage.import_data(config.country_config,
                           form_directory,
                           engine)
        results = session.query(manage.form_tables["case"])
        assert len(results.all()) == 500
        results = session.query(manage.form_tables["alert"])
        assert len(results.all()) == 500
        results = session.query(manage.form_tables["register"])
        assert len(results.all()) == 500

        manage.import_variables(config.country_config, engine)
        agg_var = session.query(model.AggregationVariables).filter(model.AggregationVariables.id == "tot_1")
        assert agg_var.first().name == "Total"

        manage.import_links(config.country_config, engine)
        link_defs = session.query(model.LinkDefinitions)
        assert link_defs.first().name == "Alert Investigation"


        manage.raw_data_to_variables(engine)
        agg_var_female = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.name == "Female").first()
        results = session.query(model.Data)
        assert len(results.all()) == 1000
        number_of_totals = 0
        number_of_female = 0
        for row in results:
            if "tot_1" in row.variables.keys():
                number_of_totals += 1
            if str(agg_var_female.id) in row.variables.keys():
                number_of_female += 1
        total = session.query(model.form_tables["case"]).filter(
            model.form_tables["case"].data.contains(
                {"intro./visit_type": 'new'}))
        female = session.query(model.form_tables["case"]).filter(
            model.form_tables["case"].data.contains(
                {"intro./visit_type": 'new', "pt1./gender": 'female'}))
        assert number_of_totals == len(total.all())
        assert number_of_female == len(female.all())
        
        manage.add_links(engine)
        link_query = session.query(model.Links)
        links = {}
        for link in link_query:
            links[link.link_value] = link
        
        alert_query = session.query(model.Alerts)
        alerts = {}
        for a in alert_query:
            alerts[a.id] = a
        alert_inv_query = session.query(model.form_tables["alert"])
        alert_invs = {}
        for a in alert_inv_query:
            alert_invs.setdefault(a.data["pt./alert_id"], [])
            alert_invs[a.data["pt./alert_id"]].append(a)

        for alert_id in alerts.keys():
            if alert_id in alert_invs.keys():
                assert alert_id in links.keys()
                if len(alert_invs[alert_id]) == 1:
                    assert(links[alert_id].to_date
                           == parse(alert_invs[alert_id][0].data["end"]))
                    labs = (alert_invs[alert_id][0]
                            .data["alert_labs./return_lab"])
                    if labs == "unsure":
                        assert "Ongoing" == links[alert_id].data["status"]
                    elif labs == "yes":
                        assert "Confirmed" == links[alert_id].data["status"]
                    elif labs == "no":
                        assert "Disregarded" == links[alert_id].data["status"]
                else:
                    investigations = alert_invs[alert_id]
                    largest_date = datetime(2015, 1, 1)
                    latest_inv = None
                    for inv in investigations:
                        if parse(inv.data["end"]) > largest_date:
                            largest_date = parse(inv.data["end"])
                            largest_inv = inv
                    assert links[alert_id].to_date == largest_date
                    labs = (largest_inv
                            .data["alert_labs./return_lab"])
                    if labs == "unsure":
                        assert "Ongoing" == links[alert_id].data["status"]
                    elif labs == "yes":
                        assert "Confirmed" == links[alert_id].data["status"]
                    elif labs == "no":
                        assert "Disregarded" == links[alert_id].data["status"]

            else:
                assert alert_id not in links.keys()
