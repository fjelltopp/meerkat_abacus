"""
Celery setup and wraper tasks to periodically update the database.
"""
from meerkat_abacus import config, util, data_management
from celery.signals import worker_ready
from datetime import datetime
import requests
import logging
import traceback
# from celery import Celery
import celery
from meerkat_abacus import celeryconfig
import raven
from raven.contrib.celery import register_signal, register_logger_signal
from meerkat_libs.logger_client import Logger

class Celery(celery.Celery):

    def on_configure(self):
        if config.sentry_dns:
            client = raven.Client(config.sentry_dns)
            # register a custom filter to filter out duplicate logs
            register_logger_signal(client)
            # hook into the Celery error handler
            register_signal(client)

        # set up logging
        logging_url = config.LOGGING_URL
        source = config.LOGGING_SOURCE
        source_type = config.LOGGING_SOURCE_TYPE
        implementation = config.LOGGING_IMPLEMENTATION
        event_type = "batch_job_event"
        self.logger = Logger(logging_url,
                        event_type,
                        source,
                        source_type,
                        implementation)


app = Celery()
app.config_from_object(celeryconfig)

from api_background.export_data import export_form, export_category, export_data


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
    app.logger.send({"task":"set_up_db"})


@app.task
def get_proccess_data(print_progress=False):
    """Get/create new data and proccess it."""
    if config.fake_data:
        if config.country_config['manual_test_data']:
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
    app.logger.send({   "task":"get_proccess_data",
                        "new_records": len(new_records),
                        "corrected_initial_visits": len(changed_records)})

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
    return data_management.new_data_to_codes(no_print=True,
                                             restrict_uuids=restrict_uuids
                                             )


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
        app.logger.send({   "task":"send_report_email", 
                            "report": str(report).
                            "status": "SUCCESS"}
                            )

    except Exception:
        # Log the exception properly.
        logging.exception(pre + "Report email request failed.")

        # Notify the developers that there has been a problem.
        data = {
            "subject": "FAILED: " + str(report) + " email",
            "message": "The {} email has failed to send.".format(report),
            "html-message": (
                "<p>Hi <<first_name>> <<last_name>>,</p>" +
                "<p>There's been a problem sending the " + str(report) +
                " report email. " + "Here's the traceback...</p><p>" +
                str(traceback.format_exc()) + "</p><p>The problem occured " +
                "at " + datetime.now().isoformat() + " for the country " +
                config.country_config.get('country_name', 'ERROR') +
                " with HERMES_DEV == " + str(config.hermes_dev) + ".</p>" +
                "<p><b>Hope you can fix it soon!</b></p>")
        }
        util.hermes('/error', 'PUT', data)
        app.logger.send({   "task":"send_report_email", 
                            "report": str(report),
                            "status": "FAILURE",
                            "data": data}
                            )
