"""
Celery configuration file

To setup celery properly we need the broker and backend urls, which
can be set by environment variable MEERKAT_BROKER_URL.

In this config we set up a beat schedule with a timedelta in seconds
given by the config.interval value.

"""
from datetime import timedelta, datetime
from celery.schedules import crontab
import logging
import os

import meerkat_abacus.config as config

BROKER_URL = 'amqp://guest@rabbit//'
CELERY_RESULT_BACKEND = 'rpc://guest@rabbit//'

new_url = os.environ.get("MEERKAT_BROKER_URL")
if new_url:
    BROKER_URL = new_url
    CELERY_RESULT_BACKEND = new_url

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_IMPORTS = ('api_background.export_data',)
CELERY_ENABLE_UTC = True
CELERYD_MAX_TASKS_PER_CHILD = 1  # To help with memory constraints


CELERYBEAT_SCHEDULE = {}
if config.start_celery:
    CELERYBEAT_SCHEDULE['get_and_proccess_data'] = {
        'task': 'task_queue.get_proccess_data',
        'schedule': timedelta(seconds=config.interval)
    }

CELERYBEAT_SCHEDULE['cleanup_downloads'] = {
    'task': 'task_queue.cleanup_downloads',
    'schedule': crontab(minute=16, hour='*')
}


# Each report will need it's own sending schedule.
# Add them from the country config to the celery schedule here.
# Only add if the mailing root is set - empty env variable silences reports.
if config.mailing_root:

    schedule = config.country_config['reports_schedule']

    for report in schedule:

        # Set up some stuff.
        task_name = 'send_' + report
        language = schedule[report]['language']
        location = schedule[report].get('location', '1')

        # Create correct crontab denoting the time the email is to be sent out.
        if schedule[report]["period"] == "week":
            send_time = crontab(
                minute=schedule[report].get("minute", 0),
                hour=schedule[report].get("hour", 4),
                day_of_week=schedule[report]["send_day"]
            )
        elif schedule[report]["period"] == "month":
            send_time = crontab(
                minute=schedule[report].get("minute", 0),
                hour=schedule[report].get("hour", 4),
                day_of_month=schedule[report]["send_day"]
            )
        else:
            send_time = crontab(
                minute=schedule[report].get("minute", 0),
                hour=schedule[report].get("hour", 4),
                day_of_week=1
            )

        # Add the email sending process to the celery schedule.
        CELERYBEAT_SCHEDULE[task_name] = {
            'task': 'task_queue.send_report_email',
            'schedule': send_time,
            'args': (report, language, location)
        }

        # If the ENV variable is set, add the report to the testing schedule.
        # Send test reports 10 minutes after this moment (once setup finished).
        # Sent every year at this time, but deployments never last that long!
        if int(config.send_test_emails):
            task_name = 'send_test_' + report
            send_time = datetime.now() + timedelta(minutes=10)
            send_time = crontab(
                    minute=send_time.minute,
                    hour=send_time.hour,
                    day_of_month=send_time.day,
                    month_of_year=send_time.month
            )
            CELERYBEAT_SCHEDULE[task_name] = {
                'task': 'task_queue.send_report_email',
                'schedule': send_time,
                'args': ('test_'+report, language, location)
            }
            # Also send the test reports every Thursday morning.
            # If these don't go out, I have a working day to debug.
            task_name = 'send_prelim_' + report
            send_time = crontab(
                minute=0,
                hour=7,
                day_of_week=4
            )
            CELERYBEAT_SCHEDULE[task_name] = {
                'task': 'task_queue.send_report_email',
                'schedule': send_time,
                'args': ('test_' + report, language, location)
            }

# Each message type will need it's own sending schedule.
# Add them from the country config to the celery schedule here.
# Only add if the messaging root is set - empty env variable silences reports.
if config.device_messaging_api:

    schedule = config.country_config.get('device_message_schedule',{})

    for message in schedule.keys():
        # Set up parameters
        task_name = 'send_device_message_' + message
        content = schedule[message]['message']
        distribution = schedule[message]['distribution']


        # Create correct crontab denoting the time the message is to be sent out.
        if schedule[message]["period"] == "week":
            send_time = crontab(
                minute=0,
                hour=8,
                day_of_week=schedule[message]["send_day"]
            )
        elif schedule[message]["period"] == "month":
            send_time = crontab(
                minute=0,
                hour=4,
                day_of_month=schedule[message]["send_day"]
            )
        else:
            send_time = crontab(
                minute=0,
                hour=4,
                day_of_week=1
            )
        # Add the message sending process to the celery schedule.
        CELERYBEAT_SCHEDULE[task_name] = {
            'task': 'task_queue.send_device_messages',
            'schedule': send_time,
            'args': (message, content, distribution)
        }

    if int(config.send_test_device_messages):
        # Add the test message sending process to the celery schedule.
        task_name = 'send_test_device_message'
        send_time = datetime.now() + timedelta(minutes=10)
        send_time = crontab(
                minute=send_time.minute,
                hour=send_time.hour,
                day_of_month=send_time.day,
                month_of_year=send_time.month
        )
        content = "Test " + str(datetime.now())
        distribution = ['/topics/demo']
        CELERYBEAT_SCHEDULE[task_name] = {
            'task': 'task_queue.send_device_messages',
            'schedule': send_time,
            'args': ('send_device_message_test', content, distribution)
        }


logging.warning("Celery is set up with the following beat schedule:\n" +
                str(CELERYBEAT_SCHEDULE))
