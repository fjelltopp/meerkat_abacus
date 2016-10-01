"""
Celery setup and wraper tasks to periodically update the database.

"""
from celery import Celery
app = Celery()

from meerkat_abacus import celeryconfig
app.config_from_object(celeryconfig)
from meerkat_abacus import config
from meerkat_abacus import data_management
from celery.signals import worker_ready
import requests, logging


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
        add_new_fake_data(5)
    if config.get_data_from_s3:
        get_new_data_from_s3()
    if print_progress:
        print("Import new data")
    new_records = import_new_data()
    if print_progress:
        print("To Code")
    new_data_to_codes(restrict_uuids=new_records)
    if print_progress:
        print("Finished")

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
def add_new_fake_data(to_add):
    """ Add new fake data """
    return data_management.add_new_fake_data(to_add)

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
    #Assemble params, we currently send all reports nationally for their default time period. 
    data = {"key": config.mailing_key} 
    headers = {'content-type': 'application/json'}
    url = config.mailing_root + report + "/" + location + "/"
    url = url.replace( '/en/', '/'+language+'/' )

    #Log request
    logging.warning( "Sending report email: " + report + 
                     "with language: " + language + " using url: " + url )  
    #Make the request and handle the response.
    try:
        r = requests.request( 'POST', url, json=data, headers=headers )
        logging.warning( "Report email: " + report + " request successfully sent.\nReponse: " + str(r) )
    except requests.exceptions.RequestException as e:
        logging.warning( "Report email: " + report + " request failed.\nException: " + str(e) ) 


