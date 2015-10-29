#!/usr/bin/python3
import argparse
import json
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
import os

from meerkat_abacus.database_util.import_locations import import_regions
from meerkat_abacus.database_util.import_locations import import_clinics
from meerkat_abacus.database_util.import_locations import import_districts
from meerkat_abacus.database_util import create_fake_data, get_deviceids
from meerkat_abacus.database_util import write_csv, read_csv

import meerkat_abacus.model as model
from meerkat_abacus.model import form_tables
from meerkat_abacus.config import DATABASE_URL, country_config, form_directory

data_directory = os.path.dirname(os.path.realpath(__file__)) + "/../data/"

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["create-db",
                                       "import-locations",
                                       "fake-data",
                                       "import-data",
                                       "import-variables",
                                       "all"],
                    help="Choose action" )
parser.add_argument("--drop-db", action="store_true",
                    help="Use flag to drop DB for create-db or all")


def create_db(url, base, country_config, drop=False):
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


def table_data_from_csv(filename, table, directory, session,
                        engine, deviceids=None, table_name=None, form=True):
    """
    Adds data to table with name

    Args:
    filename: name of table
    table: table class
    directory: directory where the csv file is
    engine: SqlAlchemy engine
    session: SqlAlchemy session
    deviceids: if we should only add rows with a one of the deviceids
    table_name: name of table if different from filename
    form: if this is a form table
    """
    session.query(table).delete()
    if not table_name:
        engine.execute("ALTER SEQUENCE {}_id_seq RESTART WITH 1;"
                       .format(filename))
    else:
        engine.execute("ALTER SEQUENCE {}_id_seq RESTART WITH 1;"
                       .format(table_name))

    session.commit()
    rows = read_csv(os.path.dirname(os.path.realpath(__file__)) + "/" +
                    directory + filename + ".csv")
    for row in rows:
        if form:
            if deviceids:
                if row["deviceid"] in deviceids:
                    session.add(table(**{"data": row,
                                         "uuid": row["meta/instanceID"]}))
            else:
                session.add(table(**{"data": row,
                                     "uuid": row["meta/instanceID"]}))
        else:
            row.pop("")
            session.add(table(**row))
        session.commit()


def import_variables(country_config, engine):
    """
    Delete current data and then import form data
    from csv files into the database.

    Args:
    country_config: configuration
    form_directory: directory to find the forms
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    table_data_from_csv(country_config["codes_file"],
                        model.AggregationVariables,
                        "../data/",
                        session, engine,
                        table_name="aggregation_variables",
                        form=False)


def import_data(country_config, form_directory, engine):
    """
    Delete current data and then import form data
    from csv files into the database.

    Args:
    country_config: configuration
    form_directory: directory to find the forms
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    deviceids_case = get_deviceids(session, case_report=True)
    deviceids = get_deviceids(session, case_report=True)
    table_data_from_csv(country_config["tables"]["case"],
                        form_tables["case"],
                        form_directory,
                        session, engine,
                        deviceids=deviceids_case)
    table_data_from_csv(country_config["tables"]["register"],
                        form_tables["register"],
                        form_directory,
                        session, engine,
                        deviceids=deviceids)
    table_data_from_csv(country_config["tables"]["alert"],
                        form_tables["alert"],
                        form_directory,
                        session, engine,
                        deviceids=deviceids)
    for table in country_config["tables"]["other"]:
        table_data_from_csv(table,
                            form_tables["other"][table],
                            form_directory,
                            session, engine,
                            deviceids=deviceids )

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
    if args.action == "import-data":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_data(country_config, form_directory, engine)
    if args.action == "import-variables":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_variables(country_config, engine)
    if args.action == "all":
        create_db(DATABASE_URL, model.Base, country_config, drop=args.drop_db)
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_locations(country_config, engine)
        fake_data(country_config, form_directory)
        import_data(country_config, form_directory, engine)
        import_variables(country_config, engine)
