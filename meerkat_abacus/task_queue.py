"""
Task queue

"""
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

from meerkat_abacus.config import country_config, form_directory, DATABASE_URL
import meerkat_abacus.celeryconfig
from meerkat_abacus import model
import meerkat_abacus.aggregation.to_codes as to_codes
from meerkat_abacus import database_util
app = Celery()
app.config_from_object(meerkat_abacus.celeryconfig)


@app.task
def import_new_data():
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    for form in model.form_tables.keys():
        file_path = (os.path.dirname(os.path.realpath(__file__)) + "/" +
                     form_directory + country_config["tables"][form] + ".csv")
        data = database_util.read_csv(file_path)
        new = database_util.add_new_data(model.form_tables[form],
                                         data, session)
    return True


@app.task
def new_data_to_codes():
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    variables = to_codes.get_variables(session)
    locations = database_util.all_location_data(session)
    results = session.query(model.Data.uuid)
    uuids = []
    for row in results:
        uuids.append(row.uuid)
    for form in model.form_tables.keys():
        result = session.query(model.form_tables[form].uuid,
                               model.form_tables[form].data)
        for row in result:
            if row.uuid not in uuids:
                new_data = to_codes.to_code(row.data,
                                            variables,
                                            locations,
                                            country_config["form_dates"][form],
                                            country_config["tables"][form])
                if new_data.variables != {}:
                    session.add(new_data)
    session.commit()
    return True
