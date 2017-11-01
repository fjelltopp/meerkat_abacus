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
import importlib.util

# Application config
DEPLOYMENT = os.environ.get("DEPLOYMENT", "unknown")
current_directory = os.path.dirname(os.path.realpath(__file__))
DATABASE_URL = os.environ.get(
    "MEERKAT_ABACUS_DB_URL",
    'postgresql+psycopg2://postgres:postgres@db/meerkat_db'
)
data_directory = os.environ.get("DATA_DIRECTORY",
                                current_directory + "/data/")
config_directory = os.environ.get("COUNTRY_CONFIG_DIR",
                                  current_directory + "/country_config/")
fake_data = int(os.environ.get("NEW_FAKE_DATA", True))
start_celery = os.environ.get("START_CELERY", False)
get_data_from_s3 = int(os.environ.get("GET_DATA_FROM_S3", False))
interval = 3600  # Seconds
hermes_api_key = os.environ.get("HERMES_API_KEY", "")
hermes_api_root = os.environ.get("HERMES_API_ROOT", "")
mailing_key = os.environ.get("MAILING_KEY", "")
mailing_root = os.environ.get("MAILING_ROOT", "")
device_messaging_api = os.environ.get("DEVICE_MESSAGING_API", "")
auth_root = os.environ.get('MEERKAT_AUTH_ROOT', 'http://nginx/auth')
send_test_emails = os.environ.get('MEERKAT_TEST_EMAILS', False)
server_auth_username = os.environ.get('SERVER_AUTH_USERNAME', 'root')
server_auth_password = os.environ.get('SERVER_AUTH_PASSWORD', 'password')
send_test_device_messages = os.environ.get('MEERKAT_TEST_DEVICE_MESSAGES',
                                           False)
sentry_dns = os.environ.get('SENTRY_DNS', '')
db_dump = os.environ.get('DB_DUMP', '')
db_dump_folder = '/var/www/dumps/'

import_fraction = float(os.environ.get("IMPORT_FRACTION", 0))
# Country config
country_config_file = os.environ.get("COUNTRY_CONFIG", "demo_config.py")

spec = importlib.util.spec_from_file_location(
    "country_config_module",
    config_directory + country_config_file
)
country_config_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(country_config_module)
country_config = country_config_module.country_config
if hasattr(country_config_module, 'dhis2_config'):
    # dhis2 export is feature toggled for now
    # proper country configs will be added after feature launch
    dhis2_config = country_config_module.dhis2_config


s3_bucket = country_config_module.s3_bucket
