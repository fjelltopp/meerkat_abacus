"""
Celery setup and wraper tasks to periodically update the database.
"""
from meerkat_abacus import config, util, data_management
from celery.signals import worker_ready
from datetime import datetime
from raven.contrib.celery import register_signal, register_logger_signal
from meerkat_abacus import celeryconfig
import requests
import logging
import traceback
import celery
import raven
import time
import os
import shutil


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

from api_background.export_data import export_form, export_category, export_data, export_data_table


# When we start celery we run the set_up_db command
@worker_ready.connect
def set_up_task(**kwargs):
    """
    Start the set_up_db task as soon as workers are ready
    """
    set_up_db.delay()


@app.task
def set_up_db():
    """
    Run the set_up_everything command from data_management.
    """
    print("Setting up DB for {}".format(
        config.country_config["country_name"]))
    data_management.set_up_everything(False,
                                      True,
                                      500)
    print("Finished setting up DB")


@app.task
def get_proccess_data(print_progress=False):
    """Get/create new data and proccess it."""
    if config.fake_data:
        if config.country_config.get('manual_test_data', None):
            add_new_fake_data(5, from_files = True)
        else:
            add_new_fake_data(5)
    if config.get_data_from_s3:
        get_new_data_from_s3()
    if print_progress:
        print("Import new data")
    new_records = import_new_data()
    if print_progress:
        print("Validating initial visits")
    changed_records = correct_initial_visits()
    if print_progress:
        print("To Code")
    new_data_to_codes(restrict_uuids=list(set(changed_records + new_records)))
    if print_progress:
        print("Finished")


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
        no_print=True,
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
        logging.info("Mailing root not set. Email {} not sent.".format(report))
        return

    # Important to log so we can debug if something goes wrong in deployment.
    pre = "EMAIL " + str(report) + ":  "
    logging.warning(pre + "Trying to send report email.")

    try:
        # Authenticate the email sending
        url = config.auth_root + '/api/login'
        data = {'username': 'report-emails', 'password': config.mailing_key}
        headers = {'content-type': 'application/json'}

        logging.warning(
            pre + "Sending authentication request to " +
            str(url) + " with headers: " + str(headers)
        )
        r = requests.request('POST', url, json=data, headers=headers)
        logging.warning(pre + "Received authentication response: " + str(r))

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
        logging.info(pre + "Sending report email for location: " +
                     str(location) + " with language: " +
                     str(language) + " using url: " + str(url) +
                     " and headers: " + str(headers))

        # Make the request and handle the response.
        r = requests.post(url, json=data, headers=headers)
        logging.info(pre + "Received email request reponse: " + str(r))

        # If the response is not a 200 OK, raise an Exception so that we can
        # handle it properly.
        if r.status_code != 200:
            raise Exception(
                "Email request returned not-ok response code: " +
                str(r.status_code)
                )

        # Report success
        logging.info(pre + "Successfully sent " + str(report) + " email.")

    except Exception:
        # Log the exception properly.
        logging.exception(pre + "Report email request failed.")

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
        util.hermes('/error', 'PUT', data)

@app.task
def send_device_messages(message, content, distribution):
    """send the device messages"""

    # If the device message root isn't set, don't send the device messages.
    if not config.device_messaging_api:
        logging.info("Device messaging root not set. Message {} not sent.".format(message))
        return

    # Important to log so we can debug if something goes wrong in deployment.
    pre = "DEVICE MESSAGE " + str(message) + ":  "
    logging.info(pre + "Trying to send device messages.")

    try:
        # Assemble params.
        url = config.device_messaging_api

        for target in distribution:
            # Log the full request so we can debug later if necessary.
            logging.info(pre + "Sending device message: " +
                         str(message) + " with content: '" +
                         str(content) + "' to " +
                         str(target))

            data = {'destination': str(target), 'message': str(content)}

            # Make the request and handle the response.
            r = util.hermes(url='gcm',method='PUT',data=data)

            logging.info(pre + "Received device messaging response: " + str(r))

            if r.status_code != 200:
                raise Exception(
                    "Device messaging returned not-ok response code: " +
                    str(r.status_code)
                    )

        # Report success
        logging.info(pre + "Successfully sent " + str(message) + " device message.")

    except Exception:
        # Log the exception properly.
        logging.exception(pre + "Device message request failed.")

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
        util.hermes('/error', 'PUT', data)
