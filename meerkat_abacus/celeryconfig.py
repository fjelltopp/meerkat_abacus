"""Celery configuration file

To setup celery properly we need the broker and backend urls, which 
can be set by environment variable MEERKAT_BROKER_URL.

In this config we set up a beat schedule with a timedelta in seconds 
given by the config.interval value.

"""
from datetime import timedelta, datetime
from celery.schedules import crontab
import logging, os

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
CELERY_IMPORTS = ('meerkat_api.background_tasks.export_data',)
CELERY_ENABLE_UTC = True
CELERYD_MAX_TASKS_PER_CHILD = 1  # To help with memory constraints


CELERYBEAT_SCHEDULE = {}
if config.start_celery:
    CELERYBEAT_SCHEDULE['get_and_proccess_data'] = {
        'task': 'task_queue.get_proccess_data',
        'schedule': timedelta(seconds=config.interval)
    }

#Each report will need it's own sending schedule. 
#Add them from the country config to the celery schedule here.
#Only add if the mailing root is set - to silence reports, empty the env variable.
if config.mailing_root:

    schedule = config.country_config['reports_schedule']
    
    for report in schedule:
        
        #Set up some stuff.
        task_name = 'send_' + report
        language = schedule[report]['language']
        location = schedule[report].get('location', '1' )

        #Create the correct crontab object denoting the time the emails is to be sent out.
        if schedule[report]["period"] == "week":
            send_time = crontab( 
                minute=0, 
                hour=4, 
                day_of_week=schedule[report]["send_day"]
            )
        elif schedule[report]["period"] == "month":
            send_time = crontab( 
                minute=0, 
                hour=4, 
                day_of_month=schedule[report]["send_day"]
            )
        else:
            send_time = crontab( 
                minute=0, 
                hour=4, 
                day_of_week=1
            )

        #Add the email sending process to the celery schedule.
        CELERYBEAT_SCHEDULE[task_name] = {
            'task': 'task_queue.send_report_email',
            'schedule': send_time,
            'args': (report, language, location)
        }
    
        #If the ENV variable is set, also add the report to the testing schedule.
        #Send the test report 15 minutes after this moment (schedule should have started).
        #It's sent every year at this time, but deployments never last that long!
        if config.send_test_emails:
            task_name = 'send_test_' + report
            send_time = datetime.now() + timedelta(minutes=10)
            send_time = crontab( 
                    minute=send_time.minute , 
                    hour=send_time.hour, 
                    day_of_month=send_time.day,
                    month_of_year=send_time.month    
            )
            CELERYBEAT_SCHEDULE[task_name] = {
                'task': 'task_queue.send_report_email',
                'schedule': send_time,
                'args': ('test_'+report, language, location)
            } 
            #Also send the test reports every Thursday morning.
            #If these don't go out, I have a working day to debug. 
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

logging.warning( "Celery is set up with the following beat schedule:\n" + str(CELERYBEAT_SCHEDULE) )

