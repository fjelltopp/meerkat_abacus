"""
Functions to create the database, populate the db tables and proccess data.

"""
import copy
import csv
import inspect
import json
import logging
import os
import os.path
import random
import subprocess
import time

import boto3
from datetime import datetime
from dateutil.parser import parse
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, Polygon, MultiPolygon
from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy import exc, over, update, delete
from sqlalchemy.orm import sessionmaker, aliased, Query
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import bindparam
from sqlalchemy_utils import database_exists, create_database, drop_database
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


from meerkat_abacus.util import data_types
import meerkat_libs as libs
from meerkat_abacus import alerts as alert_functions
from meerkat_abacus.config import config
from meerkat_abacus import model
from meerkat_abacus import util
from meerkat_abacus.codes import to_codes
from meerkat_abacus.util import create_fake_data
from meerkat_abacus.util.epi_week import epi_week_for_date
from meerkat_libs import consul_client as consul

country_config = config.country_config



def export_data(session):
    """
    Helper function to export all the data in the database.
    Prints out all the objects

    Args:
       session: db_session
    """
    for name, obj in inspect.getmembers(model):
        if inspect.isclass(obj) and hasattr(obj, "__table__"):
            for r in session.query(obj):
                columns = dict((col, getattr(r, col))
                               for col in r.__table__.columns.keys())
                logging.debug(name + "(**" + str(columns) + "),")



def import_data(engine, session, param_config=config):
    """
    Imports all the data for all the forms from the csv files

    Args:
       engine: db engine
       session: db session
    """

    deviceids_case = util.get_deviceids(session, case_report=True)
    deviceids = util.get_deviceids(session)
    start_dates = util.get_start_date_by_deviceid(session)

    country_config = param_config.country_config

    for form in model.form_tables().keys():

        uuid_field = "meta/instanceID"
        if "tables_uuid" in country_config:
            uuid_field = country_config["tables_uuid"].get(form, uuid_field)
        if form in country_config["require_case_report"]:
            form_deviceids = deviceids_case
        else:
            form_deviceids = deviceids
        if "no_deviceid" in country_config and form in country_config["no_deviceid"]:
            form_deviceids = []
        quality_control = False
        if "quality_control" in country_config:
            if form in country_config["quality_control"]:
                quality_control = True
        allow_enketo = False
        if form in country_config.get("allow_enketo", []):
            allow_enketo = country_config["allow_enketo"][form]
        exclusion_list = get_exclusion_list(session, form)
        table_data_from_csv(
            form,
            model.form_tables()[form],
            config.data_directory,
            session,
            engine,
            uuid_field=uuid_field,
            deviceids=form_deviceids,
            table_name=form,
            start_dates=start_dates,
            quality_control=quality_control,
            allow_enketo=allow_enketo,
            exclusion_list=exclusion_list,
            fraction=config.import_fraction)




def add_alerts(session, newely_inserted_data, param_config=config):
    """
    Adds non indivdual alerts.

    Individual alerts are added during the add data process.
    For any type of alert based on more than one case we add those
    alerts here.


    For each variable that should trigger a form of "threshold" alert.
    We calculate which records should make up the alert.
    We then choose the earliest alert as the representative of the whole
    alert. All the others are linked to it.

    TODO: We need to figure out a better way of dealing with the representative
    alert as there could be multiple alerts from the same day etc. Maybe we
    could generate alert_id from clinic name + week or date. Due to this
    issue we are currently not sending any threshold alert messages.

    Args:
        session: db_session


    """
    alerts = session.query(model.AggregationVariables).filter(
        model.AggregationVariables.alert == 1)

    for a in alerts.all():
        new_alerts = []
        data_type = a.type
        for newly_inserted in newely_inserted_data:
            var_id = a.id
            if var_id not in newly_inserted["variables"]:
                continue
            if not a.alert_type or a.alert_type not in ["threshold", "double"]:
                continue
            logging.info(newly_inserted)
            day = newly_inserted["date"]
            
            clinic = newly_inserted["clinic"]
                
            if a.alert_type == "threshold":
                limits = [int(x) for x in a.alert_type.split(":")[1].split(",")]
                hospital_limits = None
                if len(limits) == 4:
                    hospital_limits = limits[2:]
                    limits = limits[:2]
                new_alerts = alert_functions.threshold(
                    var_id,
                    limits,
                    session,
                    day,
                    clinic,
                    hospital_limits=hospital_limits
                    )
                type_name = "threshold"
            if a.alert_type == "double":
                new_alerts = alert_functions.double_double(a.id, day,
                                                           clinic,
                                                           session)
                type_name = "threshold"
            
        if new_alerts:
            for new_alert in new_alerts:
                # Choose a representative record for the alert
                others = new_alert["uuids"][1:]
                records = session.query(
                    model.Data, model.form_tables(param_config=param_config)[a.form]).join(
                        (model.form_tables(param_config=param_config)[a.form],
                         model.form_tables(param_config=param_config)[a.form].uuid == model.Data.uuid
                         )).filter(model.Data.uuid.in_(new_alert["uuids"]),
                                   model.Data.type == data_type)
                data_records_by_uuid = {}
                form_records_by_uuid = {}
                for r in records.all():
                    data_records_by_uuid[r[0].uuid] = r[0]
                    form_records_by_uuid[r[1].uuid] = r[1]

                for uuid in new_alert["uuids"]:
                    if uuid in data_records_by_uuid:
                        representative = uuid
                        new_variables = data_records_by_uuid[representative].variables
                        break
                else:
                    return None

                # Update the variables of the representative alert
                new_variables["alert"] = 1
                new_variables["alert_type"] = type_name
                new_variables["alert_duration"] = new_alert["duration"]
                new_variables["alert_reason"] = var_id
                new_variables["alert_id"] = data_records_by_uuid[
                    representative].uuid[-param_config.country_config["alert_id_length"]:]

                for data_var in param_config.country_config["alert_data"][a.form].keys():
                    new_variables["alert_" + data_var] = form_records_by_uuid[
                        representative].data[param_config.country_config["alert_data"][a.form][
                            data_var]]

                # Tell sqlalchemy that we have changed the variables field
                data_records_by_uuid[representative].variables = new_variables
                flag_modified(data_records_by_uuid[representative],
                              "variables")
                # Update all the non-representative rows
                for o in others:
                    data_records_by_uuid[o].variables[
                        "sub_alert"] = 1
                    data_records_by_uuid[o].variables[
                        "master_alert"] = representative

                    for data_var in param_config.country_config["alert_data"][a.form].keys():
                        data_records_by_uuid[o].variables[
                            "alert_" + data_var] = form_records_by_uuid[
                            o].data[param_config.country_config["alert_data"][a.form][data_var]]
                    flag_modified(data_records_by_uuid[o], "variables")
                session.commit()
                session.flush()
                # #send_alerts([data_records_by_uuid[representative]], session)

            new_alerts = []


def create_alert_id(alert, param_config=config):
    """
    Create an alert id based on the alert we have

    Args:
        alert: alert_dictionary

    returns:
       alert_id: an alert id

    """
    return "".join(sorted(alert["uuids"]))[-param_config.country_config["alert_id_length"]:]

import time

def add_new_fake_data(to_add, from_files=False, param_config=config):
    """
    Wrapper function to add new fake data to the existing csv files
i
    Args:
       to_add: number of new records to add
    """
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    add_fake_data(session=session, N=to_add, append=True, from_files=from_files, param_config=param_config)


def create_links(links, data, base_form, form, uuid, connection, 
                 param_config=config):
    """
    Creates all the links for a given data row
    Args:
        data_type: The data type we are working with
        input_conditions: Some data types have conditions for
                          which records qualify
        table: Class of the table we are linking from
        session: Db session
        conn: DB connection

    """
    country_config = param_config.country_config

    link_names = []
    original_form = data
    if not base_form:
        for link in links:
            if link["to_form"] != form:
                continue
            if link["to_condition"]:
                column, condition = link["to_condition"].split(":")
                if original_form[column] != condition:
                    continue
            # aggregate_condition = link['aggregate_condition']
            from_form = model.form_tables(param_config=param_config)[link["from_form"]]
            link_names.append(link["name"])

            columns = [from_form.uuid, from_form.data]
            conditions = []
            for i in range(len(link["from_column"].split(";"))):
                from_column = link["from_column"].split(";")[i]
                to_column = link["to_column"].split(";")[i]
                operator = link["method"].split(";")[i]
                if operator == "match":
                    conditions.append(from_form.data[from_column].astext ==
                                      original_form[to_column])

                elif operator == "lower_match":
                    conditions.append(
                        func.replace(func.lower(from_form.data[from_column].astext),
                                                   "-", "_") ==
                                     str(original_form.get(to_column)).lower().replace("-", "_"))
                    
                elif operator == "alert_match":
                    conditions.append(func.substring(
                                           from_form.data[from_column].astext,
                                           42 - country_config["alert_id_length"],
                                           country_config["alert_id_length"]) ==
                                      original_form[to_column])

                conditions.append(from_form.data[from_column].astext != '')
            conditions.append(from_form.uuid != uuid)

            # handle the filter condition
            
            link_query = Query(*columns).filter(*conditions)
            link_query = connection.execute(link_query.query).all()
            if len(link_query) > 1:
                logging.info(link_query)
            if len(link_query) == 0:
                return None, {}
            data = link_query[0][1]

    link_data = {}
    for link in links:
        to_form = model.form_tables(param_config=param_config)[link["to_form"]]
        link_names.append(link["name"])

        columns = [to_form.uuid, to_form.data]
        conditions = []
        for i in range(len(link["from_column"].split(";"))):
            from_column = link["from_column"].split(";")[i]
            to_column = link["to_column"].split(";")[i]
            operator = link["method"].split(";")[i]
            if operator == "match":
                conditions.append(to_form.data[to_column].astext ==
                                  data[from_column])

            elif operator == "lower_match":
                conditions.append(
                    func.replace(func.lower(to_form.data[to_column].astext),
                                               "-", "_") ==
                                 str(data[from_column]).lower().replace("-", "_"))

            elif operator == "alert_match":
                conditions.append(to_form.data[to_column].astext == \
                                  data[from_column][-country_config["alert_id_length"]:])
            conditions.append(
                   to_form.uuid != uuid)
            conditions.append(to_form.data[to_column].astext != '')

        # handle the filter condition
        if link["to_condition"]:
            column, condition = link["to_condition"].split(":")
            conditions.append(
                to_form.data[column].astext == condition)

        link_query = Query(*columns).filter(*conditions)
        link_query = connection.execute(link_query).all()
        if len(link_query) > 1:
            # Want to correctly order the linked forms
            column, method = link["order_by"].split(";")
            if method == "date":
                sort_function = lambda x: parse(x[1][column])
            else:
                sort_function = lambda x: x[1][column]
            link_query = sorted(link_query, key=sort_function)
        if len(link_query) > 0:
            link_data[link["name"]] = link_query
    return data, link_data
            



def new_data_to_codes(form, row, uuid,
                      locations,
                      links,
                      variables,
                      session,
                      engine,
                      debug_enabled=True,
                      param_config=config):
    """
    Run all the raw data through the to_codes
    function to translate it into structured data

    Args:
        engine: db engine
        debug_enabled: enables debug logging of operations
        restrict_uuids: If we should only update data related to
                       uuids in this list

    """
    country_config = param_config.country_config
    links_by_type, links_by_name = links

    data_dicts = []
    disregarded = []
    data_type_return = []
    for data_type in data_types.data_types(param_config=param_config):
        main_form = data_type["form"]
        additional_forms = []
        for link in links_by_type.get(data_type["name"], []):
            additional_forms.append(link["to_form"])
        new_data = False
        if form == main_form:
            if not check_data_type_condition(data_type, row):
                continue
            new_data = True
        elif form not in additional_forms:
            continue
#        logging.info(f"{data_type}, {form}, {main_form}, {new_data}")
        if debug_enabled:
            logging.debug("Data type: %s", data_type["type"])
        base_row, linked_records = create_links(links_by_type.get(data_type["name"], []),
                                                row, new_data, form,
                                                uuid,
                                                engine.connect(), param_config)
        if base_row is None:
            continue
        if not check_data_type_condition(data_type, base_row):
            continue
        combined_data = {main_form: base_row,
                         "links": linked_records}
        data_dict, disregarded_row = to_data(
                        combined_data, links_by_name, data_type, locations,
                        variables, session, param_config=param_config)
        for i in range(len(data_dict)):
            data_dicts.append(data_dict[i])
            disregarded.append(disregarded_row[i])
            data_type_return.append(data_type["type"])

    return data_dicts, disregarded, data_type_return


def to_data(data, links_by_name, data_type, locations, variables, session,
            param_config=config):
    """
    Constructs structured data from the entries in the data list.
    We pass the data row with all its links through the to_codes function
    to generate the list of codes for this row. We then prepare the data
    for insertion

    Args:
        data: list of data rows
        link_names: list of link names
        links_by_name: dictionary of link defs by name
        data_type: The current data type
        locations: Locations dictionary
        variables: Dict of variables

    Returns:
        data_dicts: Data to add to the Data table
        disregarded_data_dicts: Data to add to the diregarded data table
        alerts: Any new alerts added

    """





if __name__ == "__main__":
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    export_data(session)
