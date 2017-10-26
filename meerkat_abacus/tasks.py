"""
Celery setup and wraper tasks to periodically update the database.
"""
import requests
import logging
import traceback
from celery import task
import time
import json
import os
from queue import Full
import shutil
from multiprocessing import Queue
from datetime import datetime, timedelta
import pytz
import yaml

from meerkat_abacus import\
    data_management
from meerkat_abacus.config import config
import meerkat_libs as libs
from meerkat_abacus import data_import
from meerkat_abacus import util
from meerkat_abacus.pipeline import process_chunk
from meerkat_abacus.util import create_fake_data


sqs_client = None
sqs_queue_url = None

worker_buffer = Queue(maxsize=1000)

@task
def set_up_db(param_config_yaml):
    param_config = yaml.load(param_config_yaml)
    # print("param config YAML:" + param_config_yaml)
    # print("param config:" + param_config)
    data_management.set_up_database(leave_if_data=False,
                                    drop_db=True, param_config=param_config)
    if param_config.initial_data_source == "LOCAL_RDS":
        data_management.set_up_persistent_database(param_config)

@task
def initial_data_setup(source, param_config_yaml=yaml.dump(config)):
    param_config = yaml.load(param_config_yaml)
    logging.info("Starting initial setup")
    while not worker_buffer.empty():  # Make sure that the buffer is empty
        worker_buffer.get()

    engine, session = util.get_db_engine(param_config.DATABASE_URL)

    if source == "S3":
        data_import.download_data_from_s3(param_config)
        get_function = util.read_csv_filename
    elif source == "FAKE_DATA":
        get_function = util.read_csv_filename
        data_management.add_fake_data(session=session, param_config=param_config)
    elif source == "RDS":
        get_function = util.get_data_from_rds_persistent_storage

    else:
        raise AttributeError("Invalid source")
    data_import.read_stationary_data(get_function, worker_buffer,
                                     process_buffer, session, engine, param_config=param_config)
    process_buffer(internal_buffer=worker_buffer, start=False, param_config_yaml=param_config_yaml)
    session.close()
    engine.dispose()
    
@task
def test_up():
    return True

    
@task
def stream_data_from_s3(param_config_yaml=yaml.dump(config)):
    param_config = yaml.load(param_config_yaml)
    logging.info("Getting new data from S3")
    while not worker_buffer.empty():  # Make sure that the buffer is empty
        worker_buffer.get()

    engine, session = util.get_db_engine(param_config.DATABASE_URL)

    data_import.download_data_from_s3(param_config)
    get_function = util.read_csv_filename
    data_import.read_stationary_data(get_function, worker_buffer,
                                     process_buffer, session, engine, param_config=param_config)
    process_buffer(internal_buffer=worker_buffer, start=False, param_config_yaml=param_config_yaml)
    session.close()
    engine.dispose()
    stream_data_from_s3.apply_async(countdown=param_config.s3_data_stream_interval,
                               kwargs={"param_config_yaml": param_config_yaml})



@task
def process_buffer(start=True, internal_buffer=None, param_config_yaml=yaml.dump(config)):
    param_config = yaml.load(param_config_yaml)
    if internal_buffer is None:
        internal_buffer = worker_buffer
    engine, session = util.get_db_engine(param_config.DATABASE_URL)
    process_chunk(internal_buffer, session, engine, param_config)
    if start:
        process_buffer.apply_async(countdown=30,
                                   kwargs={"start": True, "param_config_yaml": param_config_yaml})
    session.close()
    engine.dispose()


@task(bind=True, default_retry_delay=300, max_retries=5)
def poll_queue(self, sqs_queue_name, sqs_endpoint, start=True, param_config_yaml=yaml.dump(config)):
    """ Get's messages from SQS queue"""
    logging.info("Running Poll Queue")

    global sqs_client
    global sqs_queue_url
    if sqs_client is None:
        sqs_client, sqs_queue_url = util.subscribe_to_sqs(sqs_endpoint,
                                                          sqs_queue_name)
    try:
        messages = sqs_client.receive_message(QueueUrl=sqs_queue_url,
                                              WaitTimeSeconds=19)
    except:
        self.retry()
    if "Messages" in messages:
        for message in messages["Messages"]:
            logging.info("Message %s", message)
            receipt_handle = message["ReceiptHandle"]
            logging.info("Deleting message %s", receipt_handle)
            try:
                message_body = json.loads(message["Body"])
                form = message_body["formId"]
                form_data = message_body["data"]
                uuid = message_body["uuid"]
                try:
                    worker_buffer.put_nowait(
                        {"form": form,
                         "uuid": uuid,
                         "data": form_data}
                    )
                except Full:
                    process_buffer(start=False, param_config_yaml=param_config_yaml)
                    worker_buffer.put(
                        {"form": form,
                         "uuid": uuid,
                         "data": form_data}
                    )
                logging.warning(worker_buffer)
                logging.info(worker_buffer.qsize())
                sqs_client.delete_message(QueueUrl=sqs_queue_url,
                                          ReceiptHandle=receipt_handle)
            except Exception as e:
                logging.exception("Error in reading message", exc_info=True)
                                    
    if start:
        poll_queue.delay(sqs_queue_name=sqs_queue_name, sqs_endpoint=sqs_endpoint,
                         start=start, param_config_yaml=param_config_yaml)


@task
def add_fake_data(N=10, interval_next=None, dates_is_now=False,
                  internal_fake_data=True, param_config_yaml=yaml.dump(config)):
    param_config = yaml.load(param_config_yaml)
    logging.info("Adding fake data")
    engine, session = util.get_db_engine(param_config.DATABASE_URL)
    for form in param_config.country_config["tables"]:
        logging.info("Generating fake data for form:" + form)
        new_data = create_fake_data.get_new_fake_data(form=form, session=session, N=N, param_config=param_config,
                                                      dates_is_now=dates_is_now)
        for row, uuid in new_data:
            if param_config.internal_fake_data:
                try:
                    worker_buffer.put_nowait(
                        {"form": form,
                         "uuid": uuid,
                         "data": row}
                    )
                except Full:
                    process_buffer(start=False, param_config_yaml=param_config_yaml)
                    worker_buffer.put(
                        {"form": form,
                         "uuid": uuid,
                         "data": row})
            elif param_config.aggregate_url:
                aggregate_config = {
                    'aggregate_url': param_config.aggregate_url,
                    'aggregate_username': param_config.aggregate_username,
                    'aggregate_password':param_config.aggregate_password
                }
                logging.info("Submitting fake data for form {0} to Aggregate".format(form))
                util.submit_data_to_aggregate(row, form, aggregate_config)
    if interval_next:
        add_fake_data.apply_async(countdown=interval_next,
                                  kwargs={"interval_next": interval_next,
                                          "N": N,
                                          "dates_is_now": dates_is_now,
                                          "internal_fake_data": internal_fake_data,
                                          "param_config_yaml": param_config_yaml})
                    
    session.close()
    engine.dispose()
        

@task
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


@task
def send_report_email(report, language, location, param_config_yaml=yaml.dump(config)):
    """Send a report email."""
    param_config = yaml.load(param_config_yaml)
    # If the mailing root isn't set, don't send the email.
    if not param_config.mailing_root:
        logging.info("Mailing root not set. Email %s not sent.".format(report))
        return

    # Important to log so we can debug if something goes wrong in deployment.
    pre = "EMAIL " + str(report) + ":  "
    logging.warning("%sTrying to send report email.", pre)

    try:
        # Authenticate the email sending
        url = param_config.auth_root + '/api/login'
        data = {'username': 'report-emails', 'password': param_config.mailing_key}
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
        url = param_config.mailing_root + report + "/" + str(location) + "/"
        url = url.replace('/en/', '/' + language + '/')

        # Log the full request so we can debug later if necessary.
        logging.warning("%sSending report email for location: %s with language: %s using url: %s and headers: %s",
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
                param_config.DEPLOYMENT
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
                deployment=param_config.DEPLOYMENT
            )
        }
        libs.hermes('/error', 'PUT', data)


@task
def send_device_messages(message, content, distribution,
                         param_config_yaml=yaml.dump(config)):

    """send the device messages"""
    param_config = yaml.load(param_config_yaml)
    # If the device message root isn't set, don't send the device messages.
    if not param_config.device_messaging_api:
        logging.info("Device messaging root not set. Message %s not sent.", message)
        return

    # Important to log so we can debug if something goes wrong in deployment.
    pre = "DEVICE MESSAGE " + str(message) + ":  "
    logging.info("%sTrying to send device messages.", pre)

    try:
        # Assemble params.
        url = param_config.device_messaging_api

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
                param_config.DEPLOYMENT
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
                deployment=param_config.DEPLOYMENT
            )
        }
        libs.hermes('/error', 'PUT', data)

