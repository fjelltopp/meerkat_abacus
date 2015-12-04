"""
Task queue

"""
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dateutil.parser import parse
import os

from meerkat_abacus.config import country_config, form_directory, DATABASE_URL
import meerkat_abacus.celeryconfig
from meerkat_abacus import model
import meerkat_abacus.aggregation.to_codes as to_codes
from meerkat_abacus import util
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
        data = util.read_csv(file_path)
        new = util.add_new_data(model.form_tables[form],
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
    locations = util.all_location_data(session)
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


@app.task
def add_new_links():
    """
    add any new data in form tables to data table
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    link_defs = util.get_link_definitions(session)
    results = session.query(model.Links)

    to_ids = []
    links_by_link_value = {}
    for row in results:
        to_ids.append(row.to_id)
        links_by_link_value[row.link_value] = row
    for link_def_id in link_defs.keys():
        link_def = link_defs[link_def_id]
        link_from = session.query(getattr(model, link_def.from_table))
        link_from_values = {}
        for row in link_from:
            link_from_values[getattr(row, link_def.from_column)] = row
        result = session.query(model.form_tables[link_def.to_table])
        for row in result:
            if row.uuid not in to_ids:
                link_to_value = row.data[link_def.to_column]
                if link_to_value in link_from_values.keys():
                    data = sort_data(link_def.data, row.data)
                    linked_record = link_from_values[link_to_value]
                    to_date = parse(row.data[link_def.to_date])
                    if link_to_value in links_by_link_value.keys():
                        old_link = links_by_link_value[link_to_value]
                        if link_def.which == "last" and old_link.to_date <= to_date:
                            old_link.data = data
                            old_link.to_date = to_date
                            old_link.to_id = getattr(row, "uuid"),
                    else:
                        new_link = model.Links(**{
                            "link_value": link_to_value,
                            "to_id": getattr(row, "uuid"),
                            "to_date": to_date,
                            "from_date": getattr(linked_record,
                                                 link_def.from_date),
                            "link_def": link_def_id,
                            "data": data})
                        links_by_link_value[link_to_value] = new_link
                        session.add(new_link)
    session.commit()
    return True


def sort_data(data_def, row):
    """
    Returns row translated to data after data_def
    
    Args:
        data_def: dictionary with data definition
        row: a row to be translated
    Returns:
        data(dict): the data
    """
    data = {}
    for key in data_def.keys():
        data[key] = []
        for value in data_def[key].keys():
            value_dict = data_def[key][value]
            if isinstance(data_def[key][value]["condition"], list):
                if row[value_dict["column"]] in value_dict["condition"]:
                    data[key].append(value)
            else:
                if value_dict["condition"] in row[value_dict["column"]].split(","):
                    data[key].append(value)
        if len(data[key]) == 1:
            data[key] = data[key][0]
    return data

                    
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
