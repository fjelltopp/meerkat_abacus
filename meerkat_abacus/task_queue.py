"""
Celery setup and wraper tasks to periodically update the database.

"""
from celery import Celery
app = Celery()
from meerkat_abacus import celeryconfig 
app.config_from_object(celeryconfig)
from meerkat_abacus import config, util, data_management
from celery.signals import worker_ready
from datetime import datetime
import requests, logging, traceback


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
    
    #Important to log properly so we can debug if something goes wrong in deployment.
    pre = "EMAIL " + str(report) + ":  "
    logging.info( pre + "Trying to send report email."  )

    try:
        #Authenticate the email sending
        url = config.auth_root + '/api/login'
        data = {'username':'report-emails', 'password':config.mailing_key}
        headers = {'content-type': 'application/json'}
     
        logging.info( pre + "Sending authentication request to " + str(url) + " with headers: " + str(headers) )
        r = requests.request('POST', url, json=data, headers=headers )
        logging.info( pre + "Received authentication request response: " + str(r) )

        #We need authentication to work, so raise an exception if it doesn't.
        if r.status_code != 200:
            raise Exception( "Authentication request returned not-ok response code: " + str(r.status_code) )

        #Create the headers for a properly authenticated request. 
        token = r.cookies['meerkat_jwt']
        headers = {**headers, **{'Authorization':'Bearer '+ token }}

        #Assemble params, we currently send all reports nationally for their default time period. 
        url = config.mailing_root + report + "/" + str(location) + "/"
        url = url.replace( '/en/', '/'+language+'/' )

        #Log the full request so we can debug later if necessary.
        logging.info( pre + "Sending report email for location: " + 
                      str(location) + " with language: " + str(language) + 
                      " using url: " + str(url) + " and headers: " + str(headers) ) 
     
        #Make the request and handle the response.
        r = requests.post( url, json=data, headers=headers )
        logging.info( pre + "Received email request reponse: " + str(r) )

        #If the response is not a 200 OK, raise an Exception so that we can handle it properly.
        if r.status_code != 200:
            raise Exception( "Email request returned not-ok response code: " + str(r.status_code) )

        #Report success
        logging.info( pre + "Successfully sent " + str(report) + " email." )

    except Exception as e:
        #Log the exception properly.
        logging.exception( pre + "Report email request failed." ) 

        #Notify the developers that there has been a problem.          
        data = {
            "id": "failed-" + str(report) + "-" + datetime.now().isoformat(),
            "topics": "error-reporting",
            "subject": "FAILED: " + str(report) + " email",
            "message": ( "<p>Hi <<first_name>> <<last_name>>,</p>" +
                         "<p>There's been a problem sending the " + str(report) + " report email. " +
                         "Here's the traceback...</p><p>" + str(traceback.format_exc()) + 
                         "</p><p>The problem occured at " + datetime.now().isoformat() + " for the " +
                         "country " + config.country_config.get('country_name', 'ERROR') + 
                         " with HERMES_DEV == " + str(config.hermes_dev) + ".</p>" +
                         "<p><b>Hope you can fix it soon!</b></p>" )
        }
        util.hermes( '/publish', 'PUT', data ) 
