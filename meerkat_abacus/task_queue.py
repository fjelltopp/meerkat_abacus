"""
Task queue

"""
from celery import Celery

import meerkat_abacus.celeryconfig
app = Celery()
app.config_from_object(meerkat_abacus.celeryconfig)


@app.task
def import_new_data(country_config, form_directory):


    return x + y
