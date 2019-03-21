"""
Functions to create the database, populate the db tables and proccess data.

"""
import copy
import csv
import inspect
import json
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

logger = config.logger
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
                logger.debug(name + "(**" + str(columns) + "),")



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
            logger.info(newly_inserted)
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












if __name__ == "__main__":
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    export_data(session)
