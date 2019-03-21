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
import yaml
from dateutil.parser import parse
import logging
# Application config
class Config:
    def __init__(self):
        # Logging
        logger_name = os.environ.get("LOGGER_NAME", "meerkat_abacus")
        logging_level = os.environ.get("LOGGING_LEVEL", "ERROR")
        logging_format = os.environ.get("LOGGING_FORMAT",  '%(asctime)s - %(name)-15s - %(levelname)-7s - %(module)s:%(filename)s:%(lineno)d - %(message)s')
        handler = logging.StreamHandler()
        formatter = logging.Formatter(logging_format)
        handler.setFormatter(formatter)
        level = logging.getLevelName(logging_level)
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)
        self.logger.addHandler(handler)
        self.logger.propagate = 0

        self.DEPLOYMENT = os.environ.get("DEPLOYMENT", "unknown")
        self.DEVELOPMENT = bool(os.environ.get("DEVELOPMENT", False))
        current_directory = os.path.dirname(os.path.realpath(__file__))
        self.DATABASE_URL = os.environ.get(
            "MEERKAT_ABACUS_DB_URL",
            'postgresql+psycopg2://postgres:postgres@db/meerkat_db'
        )
        self.data_directory = os.environ.get("DATA_DIRECTORY",
                                        current_directory + "/data/")
        self.config_directory = os.environ.get("COUNTRY_CONFIG_DIR",
                                          current_directory + "/country_config/")
        self.timezone = os.environ.get('TIMEZONE', 'Europe/Dublin')

        self.start_celery = os.environ.get("START_CELERY", False)

        self.setup = True
        self.hermes_api_key = os.environ.get("HERMES_API_KEY", "")
        self.hermes_api_root = os.environ.get("HERMES_API_ROOT", "")
        self.hermes_dev = int(os.environ.get("HERMES_DEV", False))
        self.mailing_key = os.environ.get("MAILING_KEY", "")
        self.mailing_root = os.environ.get("MAILING_ROOT", "")
        self.device_messaging_api = os.environ.get("DEVICE_MESSAGING_API", "")
        self.auth_root = os.environ.get('MEERKAT_AUTH_ROOT', 'http://nginx/auth')
        self.api_root = os.environ.get('MEERKAT_API_ROOT', 'http://nginx/api')
        self.send_test_emails = os.environ.get('MEERKAT_TEST_EMAILS', False)
        self.server_auth_username = os.environ.get('SERVER_AUTH_USERNAME', 'root')
        self.server_auth_password = os.environ.get('SERVER_AUTH_PASSWORD', 'password')
        self.send_test_device_messages = os.environ.get('MEERKAT_TEST_DEVICE_MESSAGES',
                                                        False)
        self.sentry_dns = os.environ.get('SENTRY_DNS', '')
        self.db_dump = os.environ.get('DB_DUMP', '')
        self.db_dump_folder = '/var/www/dumps/'

        self.import_fraction = float(os.environ.get("IMPORT_FRACTION", 0))
        only_import_after_date = os.environ.get("ONLY_IMPORT_AFTER", None)
        if only_import_after_date:
            self.only_import_after_date = parse(only_import_after_date)
        else:
            self.only_import_after_date = None
        self.logger.info(
            "Only importing data after {}".format(
                self.only_import_after_date)
        )

        # Country config
        country_config_file = os.environ.get("COUNTRY_CONFIG", "demo_config.py")

        spec = importlib.util.spec_from_file_location(
            "country_config_module",
            self.config_directory + country_config_file
        )
        country_config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(country_config_module)
        self.country_config = country_config_module.country_config
        if hasattr(country_config_module, 'dhis2_config'):
            # dhis2 export is feature toggled for now
            # proper country configs will be added after feature launch
            self.dhis2_config = country_config_module.dhis2_config

        if self.hermes_dev:
            self.country_config["messaging_silent"] = True

        self.s3_bucket = country_config_module.s3_bucket

        # Configure data initialisation
        self.initial_data_source = os.environ.get("INITIAL_DATA_SOURCE", "FAKE_DATA")
        self.PERSISTENT_DATABASE_URL = None
        self.get_data_from_s3 = 0
        self.s3_data_stream_interval = None
        self.initial_data = "FAKE_DATA"
        if self.initial_data_source == "FAKE_DATA":
            self.initial_data = "FAKE_DATA"
        elif self.initial_data_source == "AWS_RDS":
            self.PERSISTENT_DATABASE_URL = os.environ.get(
                "PERSISTENT_DATABASE_URL", None
            )
            self.initial_data = "RDS"
        elif self.initial_data_source == "LOCAL_RDS":
            self.PERSISTENT_DATABASE_URL = os.environ.get(
                "PERSISTENT_DATABASE_URL",
                'postgresql+psycopg2://postgres:postgres@db/persistent_demo_db'
            )
            self.initial_data = "RDS"
        elif self.initial_data_source == "AWS_S3":
            self.get_data_from_s3 = 1  # int(os.environ.get("GET_DATA_FROM_S3", False))
            self.initial_data = "S3"
        elif self.initial_data_source == "LOCAL_CSV":
            self.get_data_from_s3 = 0  # int(os.environ.get("GET_DATA_FROM_S3", False))
            self.initial_data = "LOCAL_CSV"
        else:
            msg = f"INITIAL_DATA_SOURCE={self.initial_data_source} unsupported."
            raise ValueError(msg)

        # Configure data streaming
        self.stream_data_source = os.environ.get("STREAM_DATA_SOURCE", "AWS_S3")
        if self.stream_data_source == "LOCAL_SQS":
            self.SQS_ENDPOINT = os.environ.get("SQS_ENDPOINT", 'http://172.18.0.1:9324')
            self.sqs_queue = os.environ.get("SQS_QUEUE", 'nest-queue-demo')
        elif self.stream_data_source == "AWS_SQS":
            self.SQS_ENDPOINT = os.environ.get("SQS_ENDPOINT", "DEFAULT")
            self.sqs_queue = 'nest-queue-' + self.country_config.get("implementation_id", "demo") + '-' + self.DEPLOYMENT
        elif self.stream_data_source == "AWS_S3":
            self.get_data_from_s3 = 1
            self.s3_data_stream_interval = os.environ.get("S3_DATA_STREAM_INTERVAL", 3600)
        elif self.stream_data_source == "FAKE_DATA":
            self.fake_data_generation = "INTERNAL"
        elif self.stream_data_source == "NO_STREAMING":
            pass  # Don't set up any streaming.
        else:
            msg = f"STREAM_DATA_SOURCE={self.stream_data_source} unsupported."
            raise ValueError(msg)
        # Configure generating fake data
        self.fake_data = False
        self.internal_fake_data = None
        self.fake_data_interval = 60
        self.aggregate_password = None
        self.aggregate_username = None
        self.aggregate_url = None
        self.fake_data_generation = os.environ.get("FAKE_DATA_GENERATION", None)
        if self.fake_data_generation == "INTERNAL":
            self.fake_data = True
            self.internal_fake_data = True
        elif self.fake_data_generation == "SEND_TO_AGGREGATE":
            self.fake_data = True
            self.internal_fake_data = False
            self.aggregate_password = os.environ.get("AGGREGATE_PASSWORD", "password")
            self.aggregate_username = os.environ.get("AGGREGATE_USERNAME", "test")
            self.aggregate_url = os.environ.get("AGGREGATE_URL", "http://172.18.0.1:81")
        elif self.fake_data_generation == "SEND_TO_SQS":
            self.fake_data_sqs_queue = os.environ.get("SQS_QUEUE", 'nest-queue-demo')
            self.fake_data_sqs_endpoint = os.environ.get("SQS_ENDPOINT", 'http://172.18.0.1:9324')
            self.SQS_ENDPOINT = self.fake_data_sqs_endpoint
            self.sqs_queue = self.fake_data_sqs_queue
    def __repr__(self):
        return yaml.dump(self)

config = Config()
config.logger.debug("Config initialised.")

def get_config():
    return config
