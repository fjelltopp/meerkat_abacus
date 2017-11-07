from time import sleep
import celery
import logging
import pytz
from datetime import datetime, timedelta
import raven
import copy
import yaml
import backoff


from meerkat_abacus import tasks
from meerkat_abacus import celeryconfig
from meerkat_abacus.config import config
from meerkat_abacus import util
from meerkat_abacus import data_management
from meerkat_abacus import data_import


# from meerkat_abacus.internal_buffer import InternalBuffer
from queue import Queue


@backoff.on_exception(backoff.expo,
                      (celery.exceptions.TimeoutError,
                       AttributeError, OSError),
                      max_tries=8,
                      max_value=30)
def wait_for_celery_runner():
    test_task = tasks.test_up.delay()
    result = test_task.get(timeout=1)
    return result



class Celery(celery.Celery):
    def on_configure(self):
        if config.sentry_dns:
            client = raven.Client(config.sentry_dns)
            # register a custom filter to filter out duplicate logs
            register_logger_signal(client)
            # hook into the Celery error handler
            register_signal(client)

app = Celery()
app.config_from_object(celeryconfig)
logging.getLogger().setLevel(logging.INFO)

wait_for_celery_runner()

app.control.purge()
logging.info("Setting up DB for %s", config.country_config["country_name"])
global engine
global session

tz = pytz.timezone(config.timezone)

param_config_yaml = yaml.dump(config)

tasks.set_up_db.delay(param_config_yaml=param_config_yaml).get()

logging.info("Finished setting up DB")

# Set up data initialisation
logging.info("Load data task started")
initial_data = tasks.initial_data_setup.delay(source=config.initial_data, param_config_yaml=param_config_yaml)

result = initial_data.get()
logging.info("Load data task finished")
logging.info("Starting Real time")

# Set up data stream source
if config.stream_data_source in ["LOCAL_SQS", "AWS_SQS"]:
    tasks.poll_queue.delay(config.sqs_queue, config.SQS_ENDPOINT, start=True, param_config_yaml=param_config_yaml)
elif config.stream_data_source == "AWS_S3":
    tasks.stream_data_from_s3.delay(param_config_yaml=param_config_yaml)
tasks.process_buffer.delay(start=True, param_config_yaml=param_config_yaml)


# Set up fake data generation
if config.fake_data:
    tasks.add_fake_data.apply_async(countdown=copy.deepcopy(int(config.fake_data_interval)),
                                    kwargs={"interval_next": int(copy.deepcopy(config.fake_data_interval)),
                                            "N": 4,
                                            "dates_is_now": True,
                                            "internal_fake_data": copy.deepcopy(config.internal_fake_data),
                                            "param_config_yaml": param_config_yaml
                                            })
              
while True:
    sleep(120)
