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
from datetime import datetime

from meerkat_abacus import config, data_management
import meerkat_libs as libs
from meerkat_abacus import data_import
from meerkat_abacus import util
from meerkat_abacus.pipeline import process_chunk
from meerkat_abacus.util import create_fake_data


sqs_client = None
sqs_queue_url = None

worker_buffer = Queue(maxsize=1000)

@task
def set_up_db():
    data_management.set_up_database(leave_if_data=False,
                                    drop_db=True)

@task
def initial_data_setup():
    logging.info("Starting initial setup")
    while not worker_buffer.empty():
        worker_buffer.get()
    engine, session = util.get_db_engine()
    if config.initial_data == "S3":
        data_import.download_data_from_s3(config)
        get_function = util.read_csv_filename
    elif config.initial_data == "CSV":
        get_function = util.read_csv_filename
        data_management.add_fake_data(session)
    
        data_import.read_stationary_data(get_function, worker_buffer,
                                         config, process_buffer, session, engine)
        process_buffer(internal_buffer=worker_buffer, start=False)
    process_buffer(internal_buffer=worker_buffer, start=False)
    session.close()
    engine.dispose()


@task
def process_buffer(start=True, internal_buffer=None):
    if internal_buffer is None:
        internal_buffer = worker_buffer
    engine, session = util.get_db_engine()
    process_chunk(internal_buffer, session, engine)
    if start:
        process_buffer.apply_async(countdown=30,
                                   kwargs={"start": True})
    session.close()
    engine.dispose()


@task(bind=True, default_retry_delay=300, max_retries=5)
def poll_queue(self, sqs_queue_name, sqs_endpoint, start=True):
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
                    process_buffer(start=False)
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
        poll_queue.delay(sqs_queue_name, sqs_endpoint, start=True)


@task
def add_fake_data(N=10, countdown_time=None, dates_is_now=False):
    logging.info("Adding fake data")
    engine, session = util.get_db_engine()
    for form in config.country_config["tables"]:
        logging.info("Generating fake data for form:" + form)
        new_data = create_fake_data.get_new_fake_data(form, session, N, config,
                                                      dates_is_now=dates_is_now)
        for row, uuid in new_data:
            if config.internal_fake_data:
                try:
                    worker_buffer.put_nowait(
                        {"form": form,
                         "uuid": uuid,
                         "data": row}
                    )
                except Full:
                    process_buffer(start=False)
                    worker_buffer.put(
                        {"form": form,
                         "uuid": uuid,
                         "data": row})
            elif config.aggregate_url:
                logging.info("Submitting fake data for form {0} to Aggregate".format(form))
                util.submit_data_to_aggregate(row, form, config)
    if countdown_time:
        add_fake_data.apply_async(countdown=countdown_time,
                                  kwargs={"countdown_time": countdown_time,
                                          "N": N,
                                          "dates_is_now": dates_is_now})
                    
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


@task
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

