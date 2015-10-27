#!/usr/bin/python3
import argparse
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
import os

from meerkat_abacus.database_util.import_locations import import_regions
from meerkat_abacus.database_util.import_locations import import_clinics
from meerkat_abacus.database_util.import_locations import import_districts
from meerkat_abacus.database_util import create_fake_data, get_deviceids, write_csv

import meerkat_abacus.model as model
from meerkat_abacus.config import DATABASE_URL, country_config,form_directory

data_directory = os.path.dirname(os.path.realpath(__file__)) + "/../data/"


def create_db(url, base,country_config, drop=False):
    """
    The function creates and sets up the datbase.

    Args:
    base: An SQLAlchmey declarative base with the db schema
    url : the database_url
    country_config: A contry config dictionary
    drop: Flag to drop the database before setting it up

    Returns:
    True
    """
    if drop and database_exists(url):
        drop_database(url)
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url)

    for table in country_config["tables"]:
        table_name = country_config["tables"][table]
        if table != "other":
            new_table = type(table_name, (base, ),
                             {"__tablename__": table_name,
                              "id": Column(Integer, primary_key=True),
                              "data": Column(JSONB)})
        else:
            table_names = country_config["tables"][table]
            for table in table_names:
                new_table = type(table, (base, ),
                                 {"__tablename__": table,
                                  "id": Column(Integer, primary_key=True),
                                  "data": Column(JSONB)})

    base.metadata.create_all(engine)
    return True


def fake_data(country_config, form_directory):
    """
    Creates csv files with fake data

    Args:
    country_config: A country configuration object
    form_directory: the directory to store the from data
    """
    session = Session()
    deviceids = get_deviceids(session, case_report=True)
    case = create_fake_data.create_form(country_config["fake_data"]["case"],
                                        data={"deviceids": deviceids}, N=500)
    register = create_fake_data.create_form(
        country_config["fake_data"]["register"],
        data={"deviceids": deviceids}, N=500)

    alert_ids = []
    for c in case:
        alert_ids.append(
            c["meta/instanceID"][-country_config["alert_id_length"]:])
    alert = create_fake_data.create_form(country_config["fake_data"]["alert"],
                                         data={"deviceids": deviceids, "uuids": alert_ids}, N=500)
    case_file_name = form_directory + country_config["tables"]["case"] + ".csv"
    register_file_name = (form_directory +
                          country_config["tables"]["register"] + ".csv")
    alert_file_name = (form_directory +
                       country_config["tables"]["alert"] + ".csv")

    write_csv(case, case_file_name)
    write_csv(register, register_file_name)
    write_csv(alert, alert_file_name)

    
def import_locations(country_config, engine):
    """
    Imports all locations from csv-files

    Args:
    country_config: A country configuration object
    engine: SQLAlchemy connection engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    session.query(model.Locations).delete()
    engine.execute("ALTER SEQUENCE locations_id_seq RESTART WITH 1;")
    session.add(model.Locations(name=country_config["country_name"]))
    session.commit()
    regions_file = (data_directory + "locations/" +
                    country_config["locations"]["regions"])
    districts_file = (data_directory + "locations/" +
                      country_config["locations"]["districts"])
    clinics_file = (data_directory + "locations/" +
                    country_config["locations"]["clinics"])
    import_regions(regions_file, session, 1)
    import_districts(districts_file, session)
    import_clinics(clinics_file, session, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["create-db",
                                           "import-locations",
                                           "fake-data",
                                           "all"])
    parser.add_argument("--drop-db", action="store_true")
    args = parser.parse_args()

    if args.action == "create-db":
        create_db(DATABASE_URL, model.Base, country_config, drop=args.drop_db)
    if args.action == "import-locations":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_locations(country_config, engine)
    if args.action == "fake-data":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        fake_data(country_config, form_directory)
    if args.action == "all":
        create_db(DATABASE_URL, model.Base, country_config, drop=args.drop_db)
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_locations(country_config, engine)
        fake_data(country_config, form_directory)
