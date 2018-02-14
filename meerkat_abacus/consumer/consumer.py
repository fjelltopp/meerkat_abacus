import logging
import celery
from celery import Celery
import time

from meerkat_abacus.consumer import celeryconfig
from meerkat_abacus.consumer import database_setup
from meerkat_abacus.consumer import get_data
from meerkat_abacus.config import get_config
from meerkat_abacus import util
from meerkat_abacus.util import create_fake_data
from meerkat_abacus.pipeline_worker.processing_tasks import process_data, test_up
import backoff

config = get_config()

logging.getLogger().setLevel(logging.INFO)

app = Celery()
#app.purge()
app.config_from_object(celeryconfig)
session, engine = database_setup.set_up_database(False, True, config)

@backoff.on_exception(backoff.expo,
                      (celery.exceptions.TimeoutError,
                       AttributeError, OSError),
                      max_tries=10,
                      max_value=30)
def wait_for_celery_runner():
    test_task = test_up.delay()
    result = test_task.get(timeout=1)
    return result
wait_for_celery_runner()
# Initial Setup


database_setup.unlogg_tables(config.country_config["tables"], engine)

logging.info("Starting initial setup")

if config.initial_data_source == "AWS_S3":
    get_data.download_data_from_s3(config)
    get_function = util.read_csv_filename
elif config.initial_data_source == "LOCAL_CSV":
    get_function = util.read_csv_filename
elif config.initial_data_source == "FAKE_DATA":
    get_function = util.read_csv_filename
    create_fake_data.create_fake_data(session,
                                      config,
                                      write_to="file")
 
elif config.initial_data_source in ["AWS_RDS", "LOCAL_RDS"]:
    get_function = util.get_data_from_rds_persistent_storage
else:
    raise AttributeError(f"Invalid source {config.initial_data_source}")

get_data.read_stationary_data(get_function, config)
database_setup.logg_tables(config.country_config["tables"], engine)

# Real time

while True:
    if config.stream_data_source == "AWS_S3":
        get_data.download_data_from_s3(config)
        get_data.read_stationary_data(util.read_csv_filename,
                                      session, engine, config)
        time.sleep(config.data_stream_interval)
    elif config.stream_data_source == "FAKE_DATA":
        logging.info("Sending fake data")
        if config.fake_data_generation == "INTERNAL":
            new_data = []
            for form in config.country_config["tables"]:
                data = create_fake_data.get_new_fake_data(form=form,
                                                          session=session,
                                                          N=10,
                                                          param_config=config,
                                                          dates_is_now=True)
                new_data = [{"form": form, "data": d[0]} for d in data]
            app.send_task('process_data', [new_data])
        else:
            raise NotImplementedError("Not yet implemented")
        logging.info("Sleeping")
        time.sleep(config.fake_data_interval)
