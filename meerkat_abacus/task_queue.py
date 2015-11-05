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
    """
    task to check csv files and insert any new data
    """
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
    """
    add any new data in form tables to data table
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    variables = to_codes.get_variables(session)
    locations = database_util.all_location_data(session)
    results = session.query(model.Data.uuid)
    uuids = []
    alerts = []
    for row in results:
        uuids.append(row.uuid)
    for form in model.form_tables.keys():
        result = session.query(model.form_tables[form].uuid,
                               model.form_tables[form].data)
        for row in result:
            if row.uuid not in uuids:
                new_data, alert = to_codes.to_code(
                    row.data,
                    variables,
                    locations,
                    country_config["form_dates"][form],
                    country_config["tables"][form])
                if new_data.variables != {}:
                    session.add(new_data)
                if alert:
                    alerts.append(alert)
    add_alerts(alerts, session)
    session.commit()
    return True


def add_alerts(alerts, session):
    """
    Inserts all the alerts table. If another record from the same
    day, disease and clinic already exists we create one big record.

    Args:
        uuid: uuid of case_record
        clinic: clinic id
        disease: variable id
        date: date
    """
    to_insert = {}
    for alert in alerts:
        check = str(alert.clinic) + str(alert.reason) + str(alert.date)
        if check in to_insert.keys():
            to_insert[check].uuids = ",".join(
                sorted(to_insert[check]["uuids"].split() + [alert.uuids]))
        else:
            to_insert[check] = alert
    results = session.query(model.Alerts)
    for alert in results:
        check = str(alert.clinic) + str(alert.reason) + str(alert.date)
        if check in to_insert.keys():
            alert.uuids = ",".join(
                sorted(to_insert[check]["uuids"].split() + [alert.uuids]))
            alert.id = alert.uuids[-country_config["alert_id_length"]:]
            to_insert.pop(check, None)
    for alert in to_insert.values():
        alert.id = alert.uuids[-country_config["alert_id_length"]:]
        session.add(alert)
