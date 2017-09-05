from time import sleep
import celery
import logging

from meerkat_abacus import tasks
from meerkat_abacus import celeryconfig
from meerkat_abacus import config
from meerkat_abacus import util
from meerkat_abacus import data_management
from meerkat_abacus import data_import

#from meerkat_abacus.internal_buffer import InternalBuffer
from queue import Queue


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


logging.info("Setting up DB for %s", config.country_config["country_name"])
global engine
global session
tasks.set_up_db.delay().get()

logging.info("Finished setting up DB")

logging.info("Load data task started")
initial_data = tasks.initial_data_setup.delay()

result = initial_data.get()
logging.info("Load data task finished")
logging.info("Starting Real time")
if config.connect_sqs:
    tasks.poll_queue.delay(config.sqs_queue, config.SQS_ENDPOINT, start=True)
tasks.process_buffer.delay(start=True)

if config.fake_data:
    tasks.add_fake_data.delay(N=4, countdown_time=config.fake_data_interval,
                              dates_is_now=True)
              
while True:
    sleep(120)
