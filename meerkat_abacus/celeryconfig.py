"""
Celery configuration file
"""
import meerkat_abacus.config as config
import os



BROKER_URL = 'amqp://guest@dev_rabbit_1//'
CELERY_RESULT_BACKEND = 'rpc://guest@dev_rabbit_1//'

new_url = os.environ.get("MEERKAT_BROKER_URL")
if new_url:
    BROKER_URL = new_url
    CELERY_RESULT_BACKEND = new_url

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
#CELERY_TIMEZONE = 'Europe/Oslo'
CELERY_ENABLE_UTC = True

from datetime import timedelta

CELERYBEAT_SCHEDULE = {}
if config.start_celery: 
    CELERYBEAT_SCHEDULE['get_and_proccess_data']= {
        'task': 'task_queue.get_proccess_data',
        'schedule': timedelta(seconds=config.interval)
    }

    
