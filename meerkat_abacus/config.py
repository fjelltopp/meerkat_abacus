"""
Configuration for meerkat_abacus
"""
import os
import sys
import importlib.util
def from_env(env_var, default):
    new = os.environ.get(env_var)
    if new:
        return new
    else:
        return default


# Application config
DATABASE_URL = from_env("MEERKAT_ABACUS_DB_URL",
                        'postgresql+psycopg2://postgres:postgres@db/meerkat_db')
data_directory = from_env("DATA_DIRECTORY", "~/meerkat_abacus/data/")
config_directory = from_env("COUNTRY_CONFIG_DIR",
                            "/var/www/meerkat_abacus/meerkat_abacus/country_config/")
fake_data = from_env("NEW_FAKE_DATA", True)
start_celery = from_env("START_CELERY", False)
get_data_from_s3 = from_env("DATA_FROM_S3", False)
interval = 3600  # Seconds

# Country config
country_config_file = from_env("COUNTRY_CONFIG", "demo_config.py")

spec = importlib.util.spec_from_file_location("country_config_module",
                                              config_directory + country_config_file)
country_config_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(country_config_module)
country_config = country_config_module.country_config

# import links

links_file = country_config["links_file"]
spec = importlib.util.spec_from_file_location("links",
                                              config_directory + links_file)
links = importlib.util.module_from_spec(spec)
spec.loader.exec_module(links)
