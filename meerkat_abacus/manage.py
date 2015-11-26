#!/usr/bin/python3
import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
import os
import importlib

from meerkat_abacus.database_util.import_locations import import_regions
from meerkat_abacus.database_util.import_locations import import_clinics
from meerkat_abacus.database_util.import_locations import import_districts
from meerkat_abacus.database_util import create_fake_data, get_deviceids
from meerkat_abacus.database_util import write_csv, read_csv, all_location_data

import meerkat_abacus.model as model
import meerkat_abacus.task_queue as task_queue
from meerkat_abacus.model import form_tables
from meerkat_abacus.config import DATABASE_URL, country_config, form_directory
from meerkat_abacus.aggregation.variable import Variable
import meerkat_abacus.aggregation.to_codes as to_codes

data_directory = os.path.dirname(os.path.realpath(__file__)) + "/data/"

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["create-db",
                                       "import-locations",
                                       "fake-data",
                                       "import-data",
                                       "import-variables",
                                       "import-links",
                                       "to-codes",
                                       "add-links",
                                       "all"],
                    help="Choose action" )
parser.add_argument("--drop-db", action="store_true",
                    help="Use flag to drop DB for create-db or all")
parser.add_argument("--leave-if-data", "-l", action="store_true",
                    help="A flag for all action, if there is data "
                    "in data table do nothing")
parser.add_argument("-N", type=int, default=500,
                    help="A flag for all action, if there is data "
                    "in data table do nothing")


def create_db(url, base, country_config, drop=False):
    """
    The function creates and sets up the datbase.

    Args:
        base: An SQLAlchmey declarative base with the db schema
        url : the database_url
        country_config: A contry config dictionary
        drop: Flag to drop the database before setting it up

    Returns:
        Boolean: True
    """
    if drop and database_exists(url):
        drop_database(url)
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url)
    base.metadata.create_all(engine)
    return True


def fake_data(country_config, form_directory, engine, N=500):
    """
    Creates csv files with fake data

    Args:
        country_config: A country configuration object
        form_directory: the directory to store the from data
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    deviceids = get_deviceids(session, case_report=True)
    case = create_fake_data.create_form(country_config["fake_data"]["case"],
                                        data={"deviceids": deviceids}, N=N)
    register = create_fake_data.create_form(
        country_config["fake_data"]["register"],
        data={"deviceids": deviceids}, N=N)
    form_directory = (os.path.dirname(os.path.realpath(__file__))
                      + "/" + form_directory)
    alert_ids = []
    for c in case:
        alert_ids.append(
            c["meta/instanceID"][-country_config["alert_id_length"]:])
    alert = create_fake_data.create_form(
        country_config["fake_data"]["alert"],
        data={"deviceids": deviceids, "uuids": alert_ids}, N=N)
    case_file_name = form_directory + country_config["tables"]["case"] + ".csv"
    register_file_name = (form_directory +
                          country_config["tables"]["register"] + ".csv")
    alert_file_name = (form_directory +
                       country_config["tables"]["alert"] + ".csv")

    write_csv(case, case_file_name)
    write_csv(register, register_file_name)
    write_csv(alert, alert_file_name)


def table_data_from_csv(filename, table, directory, session,
                        engine, deviceids=None, table_name=None, form=True,
                        row_function=None):
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
        row_function: function to appy to the rows before inserting
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

        if row_function:
            insert_row = row_function(row)
        else:
            insert_row = row
        if form:
            if deviceids:
                if insert_row["deviceid"] in deviceids:
                    session.add(table(**{"data": insert_row,
                                         "uuid": insert_row["meta/instanceID"]
                                     }))
            else:
                session.add(table(**{"data": insert_row,
                                     "uuid": insert_row["meta/instanceID"]}))
        else:
            insert_row.pop("")
            session.add(table(**insert_row))
        session.commit()

        
def category_to_list(row):
    """
    Transforms category to list in row
    
    Args:
        row: row of data
    Reutrns:
        row: modified row
    """
    if "," in row["category"]:
        row["category"] = [c.strip() for c in row["category"].split(",")]
    else:
        row["category"] = [row["category"]]
    return row


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
                        "data/",
                        session, engine,
                        table_name="aggregation_variables",
                        form=False,
                        row_function=category_to_list)


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
    for form in form_tables.keys():
        if form == "case":
            form_deviceids = deviceids_case
        else:
            form_deviceids = deviceids
        table_data_from_csv(country_config["tables"][form],
                            form_tables[form],
                            form_directory,
                            session, engine,
                            deviceids=form_deviceids)

        
def import_links(country_config, engine):
    """
    Imports all links from links-file

    Args:
        country_config: A country configuration object
        engine: SQLAlchemy connection engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    session.query(model.LinkDefinitions).delete()
    engine.execute("ALTER SEQUENCE link_definitions_id_seq RESTART WITH 1;")
    link_config = importlib.import_module("meerkat_abacus."+
                             country_config["links_file"] )
    for link in link_config.links:
        session.add(model.LinkDefinitions(**link))
    session.commit()
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


def raw_data_to_variables(engine):
    """
    Turn raw data in forms into structured data with codes using
    the code from the celery app.

    Args:
        engine: db engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()

    session.query(model.Data).delete()
    engine.execute("ALTER SEQUENCE data_id_seq RESTART WITH 1;")
    session.commit()
    task_queue.new_data_to_codes()
def add_links(engine):
    """
    Turn raw data in forms into structured data with codes using
    the code from the celery app.

    Args:
        engine: db engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()

    session.query(model.Links).delete()
    engine.execute("ALTER SEQUENCE links_id_seq RESTART WITH 1;")
    session.commit()
    task_queue.add_new_links()

    
def set_up_everything(url, leave_if_data, drop_db, N):
    """
    Set up everything by calling all the other functions

    Args:
        url: db url
        leave_if_data: do nothing if data is there
        drop_db: shall db be dropped before created
        N: number of data points to create
    """
    set_up = True
    if leave_if_data:
        if database_exists(url):
            engine = create_engine(url)
            Session = sessionmaker(bind=engine)
            session = Session()
            if len(session.query(model.Data).all()) > 0:
                set_up = False
    if set_up:
        create_db(url, model.Base, country_config, drop=drop_db)
        engine = create_engine(url)
        Session = sessionmaker(bind=engine)

        import_locations(country_config, engine)
        fake_data(country_config, form_directory, engine, N=N)
        import_data(country_config, form_directory, engine)
        import_variables(country_config, engine)
        import_links(country_config, engine)
        raw_data_to_variables(engine)
        add_links(engine)

    
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
        fake_data(country_config, form_directory, engine, N=args.N)
    if args.action == "import-data":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_data(country_config, form_directory, engine)
    if args.action == "import-variables":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_variables(country_config, engine)
    if args.action == "import-links":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_links(country_config, engine)
    if args.action == "to-codes":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        raw_data_to_variables(engine)
    if args.action == "add-links":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        add_links(engine)
    if args.action == "all":
        set_up_everything(DATABASE_URL,args.leave_if_data,
                          args.drop_db, args.N)
