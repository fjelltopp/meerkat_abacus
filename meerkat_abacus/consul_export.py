import json
import os
import sys

import boto3
import botocore
from time import sleep

from meerkat_libs import consul_client

from model import Base
from util import get_db_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from util.authenticate import abacus_auth_token

DB_MARKER_FILEPATH='./.db_marker_file_path.json'


def __create_table(case_form_name):
    return type(case_form_name, (Base,), {
        "__tablename__": case_form_name,
        "id": Column(Integer, primary_key=True),
        "uuid": Column(String, index=True),
        "data": Column(JSONB)
    })


tables ={
    "new_som_register": __create_table("new_som_register"),
    "new_som_case": __create_table("new_som_case")
}

s3 = boto3.resource('s3')


def get_last_read_row_marker(export_codename):
    try:
        s3.meta.client.download_file('meerkat-consul-db-markers', export_codename, DB_MARKER_FILEPATH)
        with open(DB_MARKER_FILEPATH) as f:
            marker = json.load(f)
        for form_name in tables:
            if not form_name in marker:
                marker[form_name] = 0
    except botocore.exceptions.ClientError:
        print("No db marker found at S3")
        marker = {form_name: 0 for form_name in tables}
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
    q = session.query(table).order_by(table.id).filter(table.id >= last_read_id).yield_per(1500)
    print(f"There are {q.count()} records")
    for i, row in enumerate(q):
        consul_client.send_dhis2_events(row.uuid, row.data, form_name, abacus_auth_token(), force=True)
        marker[form_name] = row.id
        if i != 0 and i % 1500 == 0:
            update_last_read_row_marker(marker, marker_aws_filename=marker_aws_filename)
            print(f"{form_name}: send {i} records.")
            sleep(300)
    update_last_read_row_marker(marker, marker_aws_filename=marker_aws_filename)


def work(argv):
    engine, session = get_db_engine()
    marker_aws_filename = get_export_codename(argv)
    print("Running the export for", marker_aws_filename)
    marker = get_last_read_row_marker(marker_aws_filename)
    for form_name, table in tables.items():
        __export_form(form_name, marker, marker_aws_filename, session, table)
    consul_client.flush_dhis2_events(abacus_auth_token())


if __name__ == '__main__':
    argv = sys.argv
    work(argv)