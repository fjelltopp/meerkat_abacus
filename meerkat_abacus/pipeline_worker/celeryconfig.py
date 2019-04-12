"""
Celery configuration file

To setup celery properly we need the broker and backend urls, which
can be set by environment variable MEERKAT_BROKER_URL.

In this config we set up a beat schedule with a timedelta in seconds
given by the config.interval value.

"""
from celery.schedules import crontab
import os

DEVELOPMENT = bool(os.environ.get("DEVELOPMENT", False))

BROKER_URL = 'amqp://guest@rabbit//'
CELERY_RESULT_BACKEND = 'rpc://guest@rabbit//'

new_url = os.environ.get("MEERKAT_BROKER_URL")
if new_url:
    BROKER_URL = new_url
    CELERY_RESULT_BACKEND = new_url

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json', 'yaml']
CELERY_ENABLE_UTC = True
# CELERYD_MAX_TASKS_PER_CHILD = 1  # To help with memory constraints


CELERYBEAT_SCHEDULE = {}
CELERYBEAT_SCHEDULE['cleanup_downloads'] = {
    'task': 'meerkat_abacus.tasks.cleanup_downloads',
    'schedule': crontab(minute=16, hour='*')
}


