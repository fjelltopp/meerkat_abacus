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
timezone = os.environ.get('TIMEZONE', 'Europe/Dublin')

start_celery = os.environ.get("START_CELERY", False)

setup = True
hermes_api_key = os.environ.get("HERMES_API_KEY", "")
hermes_api_root = os.environ.get("HERMES_API_ROOT", "")
hermes_dev = int(os.environ.get("HERMES_DEV", False))
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

if hermes_dev:
    country_config["messaging_silent"] = True

s3_bucket = country_config_module.s3_bucket

# Configure data initialisation
initial_data_source = os.environ.get("INITIAL_DATA_SOURCE", "CSV")
PERSISTENT_DATABASE_URL = None
get_data_from_s3 = None
interval = None
initial_data = None
if initial_data_source == "FAKE_DATA":
    initial_data = "FAKE_DATA"
elif initial_data_source == "AWS_RDS":
    PERSISTENT_DATABASE_URL = os.environ.get(
        "PERSISTENT_DATABASE_URL", None
    )
    initial_data = "RDS"
elif initial_data_source == "LOCAL_RDS":
    PERSISTENT_DATABASE_URL = os.environ.get(
        "PERSISTENT_DATABASE_URL",
        'postgresql+psycopg2://postgres:postgres@db/persistent_demo_db'
    )
    initial_data = "RDS"
elif initial_data_source == "S3":
    get_data_from_s3 = 1  # int(os.environ.get("GET_DATA_FROM_S3", False))
    interval = 3600  # Seconds
    initial_data = "S3"

# Configure data streaming
stream_data_source = os.environ.get("STREAM_DATA_SOURCE", "AWS_S3")
if stream_data_source == "LOCAL_SQS":
    SQS_ENDPOINT = os.environ.get("SQS_ENDPOINT", 'http://172.18.0.1:9324')
    sqs_queue = os.environ.get("SQS_QUEUE", 'nest-queue-demo')
elif stream_data_source == "AWS_SQS":
    SQS_ENDPOINT = os.environ.get("SQS_ENDPOINT", "DEFAULT")
    sqs_queue = 'nest-queue-' + country_config["country_name"] + '-' + DEPLOYMENT
elif stream_data_source == "AWS_S3":
    get_data_from_s3 = 1
    interval = 3600


# Configure generating fake data
fake_data = False
internal_fake_data = None
fake_data_interval = os.environ.get("FAKE_DATA_GENERATION", 60*5)
aggregate_password = None
aggregate_username = None
aggregate_url = None
fake_data_generation = os.environ.get("FAKE_DATA_GENERATION", None)
if fake_data_generation == "INTERNAL":
    fake_data = True
    internal_fake_data = True
elif fake_data_generation == "SEND_TO_AGGREGATE":
    fake_data = True
    internal_fake_data = False
    aggregate_password = os.environ.get("AGGREGATE_PASSWORD", "password")
    aggregate_username = os.environ.get("AGGREGATE_PASSWORD", "test")
    aggregate_url = os.environ.get("AGGREGATE_URL", "http://172.18.0.1:81")
