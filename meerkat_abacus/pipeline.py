"""
Main pipeline for abacus

"""
import logging
from collections import defaultdict
from meerkat_abacus import data_import
from meerkat_abacus import util
from meerkat_abacus.config import config
from meerkat_abacus import data_management
import time

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
    quality_control = False
    if "quality_control" in param_config.country_config:
        if form in param_config.country_config["quality_control"]:
            quality_control = True
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


def process_chunk(internal_buffer, session, engine, param_config=config,
                  run_overall_processes=True):
    """
    Processing a chunk of data from the internal buffer

    """
    uuids_form_map = defaultdict(list)
    tables = defaultdict(list)
    while internal_buffer.qsize() > 0:

        element = internal_buffer.get()
        tables[element["form"]].append(element["data"])

    forms = []
    for form in tables:
        kwargs = prepare_add_rows_arguments(form, session, param_config)
        new_uuids = data_import.add_rows_to_db(
            form,
            tables[form],
            session,
            engine,
            **kwargs)
        uuids_form_map[form] += new_uuids
        if len(new_uuids) > 0:
            forms.append(form)
    corrected = data_management.initial_visit_control(
        param_config=param_config
    )
    corrected_tables = list(
        param_config.country_config.get('initial_visit_control', {}).keys())
    if corrected_tables:
        if corrected_tables[0] in uuids_form_map:
            uuids_form_map[corrected_tables[0]] += corrected
    if len(uuids_form_map) > 0:
        data_management.new_data_to_codes(
            debug_enabled=True,
            restrict_uuids=uuids_form_map,
            param_config=param_config,
            only_forms=forms
        )
        if run_overall_processes:
            data_management.add_alerts(session, param_config=param_config)
