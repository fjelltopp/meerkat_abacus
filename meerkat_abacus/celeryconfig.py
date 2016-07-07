"""Celery configuration file

To setup celery properly we need the broker and backend urls, which 
can be set by environment variable MEERKAT_BROKER_URL.

In this config we set up a beat schedule with a timedelta in seconds 
given by the config.interval value.

"""
from datetime import timedelta
import os

import meerkat_abacus.config as config

BROKER_URL = 'amqp://guest@dev_rabbit_1//'
CELERY_RESULT_BACKEND = 'rpc://guest@dev_rabbit_1//'

new_url = os.environ.get("MEERKAT_BROKER_URL")
if new_url:
    BROKER_URL = new_url
    CELERY_RESULT_BACKEND = new_url

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_ENABLE_UTC = True
CELERYD_MAX_TASKS_PER_CHILD = 1  # To help with memory constraints


CELERYBEAT_SCHEDULE = {}
if config.start_celery:
    CELERYBEAT_SCHEDULE['get_and_proccess_data'] = {
        'task': 'task_queue.get_proccess_data',
        'schedule': timedelta(seconds=config.interval)
    }

