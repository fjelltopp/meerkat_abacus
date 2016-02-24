"""
Configuration file for meerkat_abacus

This configuration file sets up application level configurations
and imports the country specific configurations.

Many of the application level configurations can be overwritten by
environemental variables:

MEERKAT_ABACUS_DB_URL: db_url

DATA_DIRECTORY: path to directory where we store data csv files

COUNTRY_CONFIG_DIR: path to directory with country config

COUNTRY_CONFIG: name of country config file

NEW_FAKE_DATA: if we should generate fake data

GET_DATA_FROM_S3: if we should download data from an S3 bucket

START_CELERY: if we want to star the celery hourly tasks

"""
import os
import sys
import importlib.util
def from_env(env_var, default):
    """ Gets value from envrionment variable or uses default

    Args: 
        env_var: name of envrionment variable
        default: the default value
    """
    new = os.environ.get(env_var)
    if new:
        return new
    else:
        return default


# Application config
current_directory = os.path.dirname(os.path.realpath(__file__))
DATABASE_URL = from_env("MEERKAT_ABACUS_DB_URL",
                        'postgresql+psycopg2://postgres:postgres@db/meerkat_db')
data_directory = from_env("DATA_DIRECTORY",
                          current_directory + "/data/")
config_directory = from_env("COUNTRY_CONFIG_DIR",
                            current_directory + "/country_config/")
fake_data = int(from_env("NEW_FAKE_DATA", True))
start_celery = from_env("START_CELERY", False)
get_data_from_s3 = from_env("GET_DATA_FROM_S3", False)
interval = 3600  # Seconds
hermes_api_key = from_env("HERMES_API_KEY", "")
hermes_api_root = from_env("HERMES_API_ROOT", "")
hermes_silent = int(from_env("HERMES_SILENT", False))

# Country config
country_config_file = from_env("COUNTRY_CONFIG", "demo_config.py")

spec = importlib.util.spec_from_file_location("country_config_module",
                                              config_directory + country_config_file)
country_config_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(country_config_module)
country_config = country_config_module.country_config

if hermes_silent:
    country_config["messaging_silent"] = True

s3_bucket = country_config_module.s3_bucket

# import links

links_file = country_config["links_file"]
spec = importlib.util.spec_from_file_location("links",
                                              config_directory + links_file)
links = importlib.util.module_from_spec(spec)
spec.loader.exec_module(links)
