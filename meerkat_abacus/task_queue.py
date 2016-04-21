"""
Task queue

"""
from celery import Celery
app = Celery()

from meerkat_abacus import celeryconfig
app.config_from_object(celeryconfig)
from meerkat_abacus import config
from meerkat_abacus import data_management
from celery.signals import worker_ready

# When we start celery we run the set_up_db command
@worker_ready.connect
def set_up_task(**kwargs):
    set_up_db.delay()

@app.task
def set_up_db():
    """
    run the set_up_everything command from data_management
    """
    print("Setting up DB for {}".format(
        config.country_config["country_name"]))
    data_management.set_up_everything(False,
                                      True,
                                      500)
    print("Finished setting up DB")


@app.task
def get_proccess_data(print_progress=False):
    """ get/create new data and proccess it"""
    if config.fake_data:
        add_new_fake_data(5)
    if config.get_data_from_s3:
        get_new_data_from_s3()
    if print_progress:
        print("Import new data")
    import_new_data()
    if print_progress:
        print("To Code")
    new_data_to_codes()
    if print_progress:
        print("Add Links")
    add_new_links()
    if print_progress:
        print("Finished")
    
@app.task
def get_new_data_from_s3():
    """ get new data from s3"""
    data_management.get_data_from_s3(config.s3_bucket)
@app.task
def import_new_data():
    """
    task to check csv files and insert any new data
    """
    return data_management.import_new_data()
@app.task
def add_new_fake_data(to_add):
    """ Add new fake data """
    return data_management.add_new_fake_data(to_add)

@app.task
def new_data_to_codes():
    """
    add any new data in form tables to data table
    """
    return data_management.new_data_to_codes(no_print=True)


@app.task
def add_new_links():
    """ Add new links"""
    return data_management.add_new_links()
