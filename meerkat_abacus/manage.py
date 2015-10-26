from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from database_util.import_locations import import_regions
from database_util.import_locations import import_clinics
from database_util.import_locations import import_districts
import model
from config import DATABASE_URL, country_config

data_directory = "../data/"

def create_db(url, base):
    """
    The function creates and sets up the datbase.

    Args:
    base: An SQLAlchmey declarative base with the db schema
    url : the database_url

    Returns:
    True

    """
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url)
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
    import_clinics(clinics_file, session,1)

if __name__=="__main__":

    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    import_locations(country_config, engine)    

    
