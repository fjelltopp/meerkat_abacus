"""
Task queue

"""
from celery import Celery
app = Celery()
import meerkat_abacus.celeryconfig
app.config_from_object(meerkat_abacus.celeryconfig)
import meerkat_abacus.config as config
import meerkat_abacus.data_management as data_management
from celery.signals import worker_ready


@worker_ready.connect
def set_up_db(**kwargs):
    print("Setting up DB for {}".format(
        config.country_config["country_name"]))
    data_management.set_up_everything(config.DATABASE_URL,
                                      False,
                                      True,
                                      500)
    print("Finished setting up DB")

@app.task
def import_new_data():
    """
    task to check csv files and insert any new data
    """
    return data_management.import_new_data()
@app.task
def add_new_fake_data(to_add):
    return data_management.add_new_fake_data(to_add)

@app.task
def new_data_to_codes():
    """
    add any new data in form tables to data table
    """
    return data_management.new_data_to_codes()


@app.task
def add_new_links():
    return data_management.add_new_links()
