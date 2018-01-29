"""
Main pipeline for abacus

"""
import logging
from collections import defaultdict
from meerkat_abacus import data_import
from meerkat_abacus import util, model
from meerkat_abacus.config import config
from meerkat_abacus import data_management
from meerkat_abacus.codes import to_codes
import time
import random

deviceids_case = None
deviceids = None
start_dates = None
exclusion_list = None


def prepare_add_rows_arguments(form, session, param_config=config):
    global deviceids_case
    if deviceids_case is None:
        deviceids_case = util.get_deviceids(session, case_report=True)
    global deviceids
    if deviceids is None:
        deviceids = util.get_deviceids(session)
    global start_dates
    if start_dates is None:
        start_dates = util.get_start_date_by_deviceid(session)
    global exclusion_list
    if exclusion_list is None:
        exclusion_list = util.get_exclusion_list(session, form)
    
    uuid_field = "meta/instanceID"
    if "tables_uuid" in param_config.country_config:
        uuid_field = param_config.country_config["tables_uuid"].get(form, uuid_field)
    if form in param_config.country_config["require_case_report"]:
        form_deviceids = deviceids_case
    else:
        form_deviceids = deviceids
    if "no_deviceid" in param_config.country_config and form in param_config.country_config["no_deviceid"]:
        form_deviceids = []
    quality_control = {}
    quality_control_list = []
    if "quality_control" in param_config.country_config:
        if form in param_config.country_config["quality_control"]:
            (variables, variable_forms, variable_tests,
             variables_group, variables_match) = to_codes.get_variables(session, "import")
            if variables:
                quality_control_list = [variables["import"][x][x]
                            for x in variables["import"].keys() if variables["import"][x][x].variable.form == form]
            for variable in quality_control_list:
                quality_control[variable] = variable.test
                
    allow_enketo = False
    if form in param_config.country_config.get("allow_enketo", []):
        allow_enketo = param_config.country_config["allow_enketo"][form]
    return {"uuid_field": uuid_field,
            "deviceids": form_deviceids,
            "table_name": form,
            "only_new": True,
            "start_dates": start_dates,
            "quality_control": quality_control,
            "allow_enketo": allow_enketo,
            "exclusion_list": exclusion_list,
            "fraction": param_config.import_fraction,
            "only_import_after_date": param_config.only_import_after_date,
            "param_config": param_config}

import time
def process_chunk(internal_buffer, session, engine, param_config=config,
                  run_overall_processes=True):
    """
    Processing a chunk of data from the internal buffer

    """
    start = time.time()
    uuids = []
    tables = defaultdict(list)

    kwarg_by_table = {}

    locations = util.all_location_data(session)
    links = util.get_links(param_config.config_directory +
                            param_config.country_config["links_file"])

    variables = to_codes.get_variables(session)
#                                       match_on_type=data_type["type"],
#                                       match_on_form=data_type["form"])


    start = time.time()
    qc = []
    initial_visit = []
    first_db_write = []
    to_data = []
    second_db_write = []
    
    while internal_buffer.qsize() > 0:

        element = internal_buffer.get()
        form = element["form"]
        s = time.time()
        if form in kwarg_by_table:
            kwargs = kwarg_by_table[form]
        else:
            kwargs = prepare_add_rows_arguments(form,
                                                session,
                                                param_config=config)
            kwarg_by_table[form] = kwargs
        if kwargs["fraction"]:
            if random.random() > kwargs["fraction"]:
                continue

 

        data = element["data"]
        data = data_import.quality_control(
            form,
            data,
            **kwargs)
        if not data:
            continue
        #consul.flush_dhis2_events()
        qc.append(time.time() - s)
        s = time.time()
        corrected = data_management.initial_visit_control(
            form,
            data,
            engine,
            session,
            param_config=param_config
        )
        initial_visit.append(time.time() - s)
        s = time.time()
        insert_data = []
        for row in corrected:
            insert_data.append({
                "uuid": row[kwargs["uuid_field"]],
                "data": row}
            )

        #consul.send_dhis2_events(uuid=data[kwargs["uuid_field"],
        #                         form_id=corrected,
        #                         raw_row=data)

        try:
            table = model.form_tables(param_config=param_config)[form]
        except KeyError:
            logging.exception("Error in process buffer", exc_info=True)
            continue
        
        write_to_db(engine, insert_data, table=table)
        first_db_write.append(time.time() - s)
        s = time.time()
        data = []
        disregarded = []
        data_types = []
        for row in corrected:
            data_i, disregarded_i, data_types_i = data_management.new_data_to_codes(
                form,
                row,
                row[kwargs["uuid_field"]],
                locations,
                links,
                variables,
                session,
                engine,
                debug_enabled=True,
                param_config=param_config,
            )
            data += data_i
            disregarded += disregarded_i
            data_types += data_types_i
        to_data.append(time.time() -s )
        s = time.time()
        for i in range(len(data)):
            write_to_db(engine, data[i],
                        table=[model.Data, model.DisregardedData][disregarded[i]],
                        delete=("type", data_types[i]))
        second_db_write.append(time.time() - s)
        data_management.add_alerts(session, data, row=data,
                                   param_config=param_config)

        
    end = time.time() - start #after_insert - after_qc - start
    logging.info(end)
    qc_m = statistics.mean(qc)
    initial_visit_m = statistics.mean(initial_visit)
    first_db_write_m = statistics.mean(first_db_write)
    to_data_m = statistics.mean(to_data)
    second_db_write_m = statistics.mean(second_db_write)
    logging.info(f"{qc_m}, {initial_visit_m}, {first_db_write_m}, {to_data_m}, {second_db_write_m}")
    import sys
    sys.exit()
import statistics

def write_to_db(engine, data, table, delete=False):
    conn = engine.connect()

    if delete:
        conn.execute(table.__table__.delete().where(
            table.__table__.c.uuid == data["uuid"]).where(
                getattr(table.__table__.c, delete[0]) == delete[1])
        )
 
    if len(data) > 0:
        conn.execute(table.__table__.insert(), data)
    conn.close()
