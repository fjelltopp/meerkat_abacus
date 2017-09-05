"""
Main pipelin for abacus

"""
import logging
from collections import defaultdict
from meerkat_abacus import data_import
from meerkat_abacus import util
from meerkat_abacus import config
from meerkat_abacus import data_management

deviceids_case = None
deviceids = None
start_dates = None
exclusion_list = None


def prepare_add_rows_arguments(form, session):
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
    if "tables_uuid" in config.country_config:
        uuid_field = config.country_config["tables_uuid"].get(form, uuid_field)
    if form in config.country_config["require_case_report"]:
        form_deviceids = deviceids_case
    else:
        form_deviceids = deviceids
    if "no_deviceid" in config.country_config and form in config.country_config["no_deviceid"]:
        form_deviceids = []
    quality_control = False
    if "quality_control" in config.country_config:
        if form in config.country_config["quality_control"]:
            quality_control = True
    allow_enketo = False
    if form in config.country_config.get("allow_enketo", []):
        allow_enketo = config.country_config["allow_enketo"][form]
    return {"uuid_field": uuid_field,
            "deviceids": form_deviceids,
            "table_name": form,
            "start_dates": start_dates,
            "quality_control": quality_control,
            "allow_enketo": allow_enketo,
            "exclusion_list": exclusion_list,
            "fraction": config.import_fraction}


def process_chunk(internal_buffer, session, engine):
    """
    Processing a chunk of data from the internal buffer

    """
    logging.info("Processing Chunk")

    uuids = []
    tables = defaultdict(list)
    logging.info(internal_buffer.qsize())
    while internal_buffer.qsize() > 0:

        element = internal_buffer.get()
        tables[element["form"]].append(element["data"])
    for form in tables:
        kwargs = prepare_add_rows_arguments(form, session)
        uuids += data_import.add_rows_to_db(
            form,
            tables[form],
            session,
            engine,
            **kwargs)
    data_management.initial_visit_control()
    data_management.new_data_to_codes(
        debug_enabled=True,
        restrict_uuids=uuids
    )
