"""
Celery setup and wraper tasks to periodically update the database.
"""
from meerkat_abacus import config, data_management
from celery.signals import worker_ready
from datetime import datetime
from raven.contrib.celery import register_signal, register_logger_signal
from meerkat_abacus import celeryconfig
import meerkat_libs as libs
from meerkat_abacus import model
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from meerkat_abacus import util

import requests
import logging
import traceback
import celery
import raven
import time
import json
import os
import shutil
import boto3
from botocore.exceptions import ClientError

class Celery(celery.Celery):

    def on_configure(self):
        if config.sentry_dns:
            client = raven.Client(config.sentry_dns)
            # register a custom filter to filter out duplicate logs
            register_logger_signal(client)
            # hook into the Celery error handler
            register_signal(client)

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


region_name = "eu-west-1"
logger.info("Connecting to SQS")
sqs_client = boto3.client('sqs', region_name=region_name,
                          endpoint_url=config.SQS_ENDPOINT)

logger.info("Getting SQS url")
try: 
    queue_url = sqs_client.get_queue_url(
        QueueName=config.sqs_queue,
        QueueOwnerAWSAccountId=""
    )['QueueUrl']
    logger.info("Subscribed to %s.", queue_url)
except ClientError as e:
    print(e)
    logger.info("Creating Queue")
    response = sqs_client.create_queue(
        QueueName=config.sqs_queue
    )
    queue_url = sqs_client.get_queue_url(
        QueueName=config.sqs_queue,
        QueueOwnerAWSAccountId=""
    )['QueueUrl']
    logger.info("Subscribed to %s.", queue_url)
    
app = Celery()
app.config_from_object(celeryconfig)


from api_background.export_data import export_form, export_category, export_data, export_data_table


# When we start celery we run the set_up_db command
@worker_ready.connect
def set_up_task(**kwargs):
    """
    Start the set_up_db task as soon as workers are ready
    """
    set_up_db.delay()
    #poll_queue.delay()


@app.task
def poll_queue():
    """ Get's messages from SQS queue"""
    logging.info("Running Poll Queue")
    messages = sqs_client.receive_message(QueueUrl=queue_url,
                                          WaitTimeSeconds=19)
    if "Messages" in messages:
        for message in messages["Messages"]:
            logging.info("Message %s",message)
            receipt_handle = message["ReceiptHandle"]
            logging.info("Deleting message %s", receipt_handle)
            try:
                message_body = json.loads(message["Body"])
                form = message_body["formId"]
                form_data = message_body["data"]
                add_data_row.delay(form, form_data)
                sqs_client.delete_message(QueueUrl=queue_url,
                                          ReceiptHandle=receipt_handle)
            except Exception as e:
                logging.exception("Error", exc_info=True)
    poll_queue.delay()


@app.task
def add_data_row(form, form_data):
    if form not in model.form_tables:
        return None
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    deviceids_case = util.get_deviceids(session, case_report=True)
    deviceids = util.get_deviceids(session)
    start_dates = util.get_start_date_by_deviceid(session)
    uuid_field = "meta/instanceID"
    if "tables_uuid" in config.country_config:
        uuid_field = config.country_config["tables_uuid"].get(form, uuid_field)
    if form in config.country_config["require_case_report"]:
        form_deviceids = deviceids_case
    else:
        form_deviceids = deviceids
    if "no_deviceid" in config.country_config and form in config.country_config["no_deviceid"]:
        form_deviceids = []
    quality_control = False
    if "quality_control" in config.country_config:
        if form in config.country_config["quality_control"]:
            quality_control = True
    allow_enketo = False
    if form in config.country_config.get("allow_enketo", []):
        allow_enketo = config.country_config["allow_enketo"][form]
    exclusion_list = data_management.get_exclusion_list(session, form)
    restrict_uuids = data_management.add_rows_to_db(
        form,
        [form_data],
        session,
        engine,
        uuid_field=uuid_field,
        deviceids=form_deviceids,
        table_name=form,
        start_dates=start_dates,
        quality_control=quality_control,
        allow_enketo=allow_enketo,
        exclusion_list=exclusion_list,
        fraction=config.import_fraction)
    new_data_to_codes.delay(restrict_uuids=restrict_uuids)
    
@app.task
def set_up_db():
    """
    Run the set_up_everything command from data_management.
    """
    logging.debug("Setting up DB for %s", config.country_config["country_name"])
    data_management.set_up_everything(leave_if_data=False, drop_db=True, N=500)
    logging.debug("Finished setting up DB")
    poll_queue.delay()



@app.task
def correct_initial_visits():
    """
    Make sure patients don't have several initial visits
    for the same diagnosis and remove the data table
    rows for amended rows
    """
    ret = data_management.initial_visit_control()
    return ret


@app.task
def get_new_data_from_s3():
    """Get new data from s3."""
    data_management.get_data_from_s3(config.s3_bucket)


@app.task
def import_new_data():
    """
    Task to check csv files and insert any new data.
    """
    return data_management.import_new_data()


@app.task
def add_new_fake_data(to_add, from_files=False):
    """ Add new fake data """
    return data_management.add_new_fake_data(to_add, from_files)


@app.task
def new_data_to_codes(restrict_uuids=None):
    """
    Add any new data in form tables to data table.
    """
    return data_management.new_data_to_codes(
        debug_enabled=True,
        restrict_uuids=restrict_uuids
    )


@app.task
def cleanup_downloads():
    folder = '/var/www/meerkat_api/api_background/api_background/exported_data'
    downloads = os.listdir(folder)
    oldest = time.time() - 3600
    for download in downloads:
        path = '{}/{}'.format(folder, download)
        if os.stat(path).st_mtime < oldest:
            try:
                shutil.rmtree(path)
            except NotADirectoryError:
                pass


@app.task
def send_report_email(report, language, location):
    """Send a report email."""

    # If the mailing root isn't set, don't send the email.
    if not config.mailing_root:
        logging.info("Mailing root not set. Email %s not sent.".format(report))
        return

    # Important to log so we can debug if something goes wrong in deployment.
    pre = "EMAIL " + str(report) + ":  "
    logging.warning("%sTrying to send report email.", pre)

    try:
        # Authenticate the email sending
        url = config.auth_root + '/api/login'
        data = {'username': 'report-emails', 'password': config.mailing_key}
        headers = {'content-type': 'application/json'}

        logging.warning("%sSending authentication request to %s with headers: %s", pre, str(url), str(headers) )
        r = requests.request('POST', url, json=data, headers=headers)
        logging.warning("%sReceived authentication response: %s", pre,  str(r))

        # We need authentication to work, so raise an exception if it doesn't.
        if r.status_code != 200:
            raise Exception(
                "Authentication request returned not-ok response code: " +
                str(r.status_code)
            )

        # Create the headers for a properly authenticated request.
        token = r.cookies['meerkat_jwt']
        headers = {**headers, **{'Authorization': 'Bearer ' + token}}

        # Assemble params.
        # We currently send all reports nationally for the default time period.
        url = config.mailing_root + report + "/" + str(location) + "/"
        url = url.replace('/en/', '/' + language + '/')

        # Log the full request so we can debug later if necessary.
        logging.info("%sSending report email for location: %s with language: %s using url: %s and headers: %s",
                     pre, str(location), str(language), str(url), str(headers))

        # Make the request and handle the response.
        r = requests.post(url, json=data, headers=headers)
        logging.info("%sReceived email request reponse: %s", pre, str(r))

        # If the response is not a 200 OK, raise an Exception so that we can
        # handle it properly.
        if r.status_code != 200:
            raise Exception(
                "Email request returned not-ok response code: " +
                str(r.status_code)
                )

        logging.info("%sSuccessfully sent %s email.", pre, str(report))

    except Exception:
        logging.exception("%sReport email request failed.", pre, exc_info=True)

        # Notify the developers that there has been a problem.
        data = {
            "subject": "FAILED: {} email".format(report),
            "message": "Email failed to send from {} deployment.".format(
                config.DEPLOYMENT
            ),
            "html-message": (
                "<p>Hi <<first_name>> <<last_name>>,</p><p>There's been a "
                "problem sending the {report} report email. Here's the "
                "traceback...</p><p>{traceback}</p><p>The problem occured "
                "at {time} for the {deployment} deployment.</p><p><b>Hope you "
                "can fix it soon!</b></p>"
            ).format(
                report=report,
                traceback=traceback.format_exc(),
                time=datetime.now().isoformat(),
                deployment=config.DEPLOYMENT
            )
        }
        libs.hermes('/error', 'PUT', data)


@app.task
def send_device_messages(message, content, distribution):
    """send the device messages"""

    # If the device message root isn't set, don't send the device messages.
    if not config.device_messaging_api:
        logging.info("Device messaging root not set. Message %s not sent.", message)
        return

    # Important to log so we can debug if something goes wrong in deployment.
    pre = "DEVICE MESSAGE " + str(message) + ":  "
    logging.info("%sTrying to send device messages.", pre)

    try:
        # Assemble params.
        url = config.device_messaging_api

        for target in distribution:
            # Log the full request so we can debug later if necessary.
            logging.info("%sSending device message: %s with content: '%s' to %s",
                         pre, str(message), str(content), str(target))

            data = {'destination': str(target), 'message': str(content)}

            # Make the request and handle the response.
            r = libs.hermes(url='/gcm',method='PUT',data=data)
            logging.info("%sReceived device messaging response: %s", pre, str(r))

    except Exception:
        logging.exception("%sDevice message request failed.", pre, exc_info=True)

        # Notify the developers that there has been a problem.
        data = {
            "subject": "FAILED: {} device message".format(message),
            "message": "Device message failed to send from {} deployment.".format(
                config.DEPLOYMENT
            ),
            "html-message": (
                "<p>Hi <<first_name>> <<last_name>>,</p><p>There's been a "
                "problem sending the {message} device message. Here's the "
                "traceback...</p><p>{traceback}</p><p>The problem occured "
                "at {time} for the {deployment} deployment.</p><p><b>Hope you "
                "can fix it soon!</b></p>"
            ).format(
                message=message,
                traceback=traceback.format_exc(),
                time=datetime.now().isoformat(),
                deployment=config.DEPLOYMENT
            )
        }
        libs.hermes('/error', 'PUT', data)
