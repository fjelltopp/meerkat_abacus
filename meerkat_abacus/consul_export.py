import json
import logging
import os
import sys
import pathlib

import boto3
import botocore
from time import sleep

from meerkat_libs import consul_client
from meerkat_abacus.util import get_db_engine
from meerkat_abacus.util.authenticate import abacus_auth_token
import meerkat_abacus.model as abacus_model

from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

logger = logging.getLogger(__name__)


def set_logging_level():
    if config.PRODUCTION:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.DEBUG)


DB_MARKER_DIR = 'build'
DB_MARKER_FILENAME = 'db_marker_file_path.json'
DB_MARKER_FILEPATH = os.path.join(DB_MARKER_DIR, DB_MARKER_FILENAME)
form_tables = {}
config = {}

def __create_table(case_form_name):
    return type(case_form_name, (abacus_model.Base,), {
        "__tablename__": case_form_name,
        "id": Column(Integer, primary_key=True),
        "uuid": Column(String, index=True),
        "data": Column(JSONB)
    })


def __get_form_tables():
    country_config = config.country_config
    try:
        forms_to_export = country_config["consul_export_config"]["forms"]["dhis2"]
    except KeyError:
        raise Exception("Could not read dhis2 export config for consul in the country config.")
    # return {form_name: __create_table(form_name) for form_name in forms_to_export}
    all_form_tables = abacus_model.form_tables(config)
    return {form_name: all_form_tables[form_name] for form_name in forms_to_export}


s3 = boto3.resource('s3')


def get_last_read_row_marker(export_codename):
    try:
        pathlib.Path(DB_MARKER_DIR).mkdir(parents=True, exist_ok=True)
        s3.meta.client.download_file('meerkat-consul-db-markers', export_codename, DB_MARKER_FILEPATH)
        with open(DB_MARKER_FILEPATH) as f:
            marker = json.load(f)
        for form_name in form_tables:
            if not form_name in marker:
                marker[form_name] = 0
    except botocore.exceptions.ClientError:
        logger.info("No db marker found at S3")
        marker = {form_name: 0 for form_name in form_tables}
    return marker


def update_last_read_row_marker(marker, marker_aws_filename):
    with open(DB_MARKER_FILEPATH, 'w') as f:
        json.dump(marker, f)
    s3.meta.client.upload_file(DB_MARKER_FILEPATH, 'meerkat-consul-db-markers', marker_aws_filename)


def get_export_codename(argv):
    if len(argv) < 1:
        return 'unknown-test-run.json'
    return f"{argv[1]}.json"


def __export_form(form_name, marker, marker_aws_filename, session, table):
    last_read_id = marker[form_name]
    q = session.query(table).order_by(table.id).filter(table.id >= last_read_id).yield_per(2000)
    logger.info(f"There are {q.count()} records")
    for i, row in enumerate(q):
        consul_client.send_dhis2_events(row.uuid, row.data, form_name, abacus_auth_token(), force=True)
        marker[form_name] = row.id
        if i != 0 and i % 100 == 0:
            update_last_read_row_marker(marker, marker_aws_filename=marker_aws_filename)
            logger.info(f"{form_name}: send {i} records.")
            sleep(10)

    update_last_read_row_marker(marker, marker_aws_filename=marker_aws_filename)


def work(argv):
    set_logging_level()
    global form_tables
    form_tables = __get_form_tables()
    if not consul_client.wait_for_consul_start():
        logger.error("Failed to get a response from consul")
        return
    engine, session = get_db_engine()
    marker_aws_filename = get_export_codename(argv)
    logger.info("Running the export for %s", marker_aws_filename)
    marker = get_last_read_row_marker(marker_aws_filename)
    for form_name, table in form_tables.items():
        logger.info("Exporting form %s started.", form_name)
        __export_form(form_name, marker, marker_aws_filename, session, table)
        logger.info("Exporting form %s finished.", form_name)
    consul_client.flush_dhis2_events(abacus_auth_token())


def celery_trigger(param_config):
    global config
    config = param_config
    if config.PRODUCTION:
        sufix = 'production'
    else:
        sufix = 'development'
    argv = ['', f"{config.country_config['country_name']}-{sufix}"]
    work(argv)

if __name__ == '__main__':
    from meerkat_abacus.config import config as meerkat_config

    config = meerkat_config
    argv = sys.argv
    work(argv)