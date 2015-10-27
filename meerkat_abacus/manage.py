#!/usr/bin/python3
import argparse
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from database_util.import_locations import import_regions
from database_util.import_locations import import_clinics
from database_util.import_locations import import_districts
import model
from config import DATABASE_URL, country_config

data_directory = "../data/"


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
    if drop:
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


def import_locations(country_config, engine):
    """
    Imports all locations from csv-files

    Args:
    country_config: A country configuration object
    engine: SQLAlchemy connection engine
    """
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
                                           "all"])
    parser.add_argument("--drop-db", action="store_true")
    args = parser.parse_args()

    if args.action == "create-db":
        create_db(DATABASE_URL, model.Base, country_config, drop=args.drop_db)
    if args.action == "import-locations":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_locations(country_config, engine)
    if args.action == "all":
        create_db(DATABASE_URL, model.Base, country_config, drop=args.drop_db)
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_locations(country_config, engine)
