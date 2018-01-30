"""
Various utility functions for meerkat abacus
"""

import csv
import itertools
import logging
import boto3
from xmljson import badgerfish as bf
from lxml.html import Element, tostring
import requests
from requests.auth import HTTPDigestAuth
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from botocore.exceptions import ClientError

from datetime import datetime, timedelta
from dateutil.parser import parse
from jinja2 import Environment, FileSystemLoader, select_autoescape

from meerkat_abacus.model import Locations, AggregationVariables, Devices, form_tables
from meerkat_abacus.config import config
import meerkat_libs as libs
from meerkat_abacus.model import Locations, AggregationVariables, Devices

country_config = config.country_config

# Alert messages are rendered with Jinja2, setup the Jinja2 env
env = None


def get_env(param_config=config):
    global env
    if env:
        return env
    else:
        env = Environment(
            loader=FileSystemLoader(param_config.config_directory + 'templates/'),
            autoescape=select_autoescape(['html'])
        )
        return env





def is_child(parent, child, locations):
    """
    Determines if child is child of parent

    Args:
        parent: parent_id
        child: child_id
        locations: all locations in dict

    Returns:
       is_child(Boolean): True if child is child of parent
    """
    parent = int(parent)
    child = int(child)

    if child == parent or parent == 1:
        return True
    loc_id = child

    while loc_id != 1:
        loc_id = locations[loc_id].parent_location
        if loc_id == parent:
            return True
    return False


def get_db_engine(db_url=config.DATABASE_URL):
    """
    Returns a db engine and session
    """
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session


def field_to_list(row, key):
    """
    Transforms key in row to a list. We split on semicolons if they exist in
    the string, otherwise we use commas.

    Args:
        row: row of data
        key: key for the field we want
    Reutrns:
        row: modified row
    """
    if not row[key]:
        return row
    if ";" in row[key]:
        row[key] = [c.strip() for c in row[key].split(";")]
    elif "," in row[key]:
        row[key] = [c.strip() for c in row[key].split(",")]
    else:
        row[key] = [row[key]]
    return row


def get_links(file_path):
    """
    Returns links indexed by type

    """
    links = read_csv(file_path)
    links_by_type = {}
    links_by_name = {}
    for l in links:
        links_by_type.setdefault(l["type"], [])
        links_by_type[l["type"]].append(l)
        links_by_name[l["name"]] = l
    return links_by_type, links_by_name


def all_location_data(session):
    """
    Returns all location data, which is all locations indexed by location_id,
    locations by deviceid, regions and districts

    Args:
        session: db session

    Returns:
        locations(tuple): (loction_dict,loc_by_deviceid, regions, districts)
    """
    locations = get_locations(session)
    locations_by_deviceid = get_locations_by_deviceid(session)
    zones, regions, districts = get_zones_regions_districts(session)

    devices = get_device_tags(session)
    return (locations, locations_by_deviceid, zones, regions, districts, devices)


def get_variables(session):
    """
    Returns a list of aggregation variables indexed by the variable_id

    Args:
        session: db-session

    Returns:
        variables(dict): dict of id:Variable
    """
    result = session.query(AggregationVariables)
    variables = {}
    for row in result:
        variables[row.id] = row
    return variables


def get_device_tags(session):
    """
    Returns a dict of device tags by id

    Args:
        session: db-session

    Returns:
       devices(dict): dict of device_id:tags
    """
    result = session.query(Devices)
    devices = {}
    for row in result:
        devices[row.device_id] = row.tags
    return devices


def get_zones_regions_districts(session):
    """
    get list of ids for regions and districts

    Args:
        session: db session

    Returns:
        regions_district(tuple): (zones, regions, districts)
    """
    locations = get_locations(session)
    zones = []
    regions = []
    districts = []
    for l in locations.keys():
        if locations[l].level == "zone":
            zones.append(l)
        elif locations[l].level == "region":
            regions.append(l)
    for l in locations.keys():
        if locations[l].parent_location in regions and locations[l].level == "district":
            districts.append(l)
    return zones, regions, districts


def get_locations_by_deviceid(session):
    """
    get a dict with deviceid: location_id

    Args:
        session: db session

    Returns:
        locations(dict) : deviceid:location_id
    """
    locations = get_locations(session)
    locations_by_deviceid = {}
    for l in locations.keys():
        if locations[l].deviceid:
            if "," in locations[l].deviceid:
                dev_ids = locations[l].deviceid.split(",")
                for did in dev_ids:
                    locations_by_deviceid[did] = l
            else:
                locations_by_deviceid[locations[l].deviceid] = l
    return locations_by_deviceid


def get_start_date_by_deviceid(session):
    """
    get a dict with deviceid: start_date


    Args:
        session: db session

    Returns:
        locations(dict) : deviceid:start_date
    """
    locations = get_locations(session)
    locations_by_deviceid = get_locations_by_deviceid(session)
    start_date_by_deviceid = {}
    for l in locations_by_deviceid:
        start_date_by_deviceid[l] = locations[
            locations_by_deviceid[l]].start_date
    return start_date_by_deviceid


def get_locations(session):
    """
    get locations indexed by location_id

    Args:
        session: db session

    Returns:
        locations(dict) : id:location dict
    """
    result = session.query(Locations)
    locations = {}
    for row in result:
        locations[row.id] = row
    # if row.area is not None:
    #        row.area = to_shape(row.area)
    return locations


def get_deviceids(session, case_report=False):
    """
    Returns a list of deviceids

    Args:
        session: SQLAlchemy session
        case_report: flag to only get deviceids from case
                     reporing clinics
    Returns:
        list_of_deviceids(list): list of deviceids
    """
    if case_report:
        result = session.query(Locations).filter(
            Locations.case_report == 1)
    else:
        result = session.query(Locations)
    deviceids = []
    for r in result:
        if r.deviceid:
            if "," in r.deviceid:
                for deviceid in r.deviceid.split(","):
                    deviceids.append(deviceid)
            else:
                deviceids.append(r.deviceid)
    return deviceids


def write_csv(rows, file_path, mode='w'):
    """
    Writes rows to csvfile

    Args:
        rows: list of dicts with data
        file_path: path to write file to
        mode: 'w' for writing to a new file, 'a' for
         appending without overwriting

    """
    # Only write if rows were inserted
    if rows:
        with open(file_path, mode, encoding='utf-8') as f:
            columns = sorted(list(rows[0]))
            out = csv.DictWriter(f, columns)

            if mode == 'w':
                out.writeheader()

            for row in rows:
                out.writerow(row)


def write_to_db(data, form, db_url, param_config=config):
    """
    Writes data to db_url

    Args:
        rows: list of dicts with data
        form: form to write to
        db_url: Which db url to use
    """
    if data:
        dicts = []
        uuid_field = "meta/instanceID"
        if "tables_uuid" in param_config.country_config:
            uuid_field = param_config.country_config["tables_uuid"].get(form, uuid_field)
        for d in data:
            dicts.append(
                {
                    "data": d,
                    "uuid": d[uuid_field]
                }
            )
        table = form_tables(param_config=param_config)[form]
        engine, session = get_db_engine(db_url)
        conn = engine.connect()
        conn.execute(table.__table__.insert(), dicts)
        conn.close()


        
def get_exclusion_list(session, form):
    """
    Get exclusion list for a form

    Args:
        session: db session
        form: which form to get the exclusion list for
    """
    exclusion_lists = config.country_config.get("exclusion_lists", {})
    ret = []

    for exclusion_list_file in exclusion_lists.get(form, []):
        exclusion_list = read_csv(config.config_directory + exclusion_list_file)
        for uuid_to_be_removed in exclusion_list:
            ret.append(uuid_to_be_removed["uuid"])

    return ret


def read_csv_filename(filename, param_config=config):
    """ Read a csv file from the filename"""
    file_path = param_config.data_directory + filename + ".csv"
    for row in read_csv(file_path):
        yield row


def get_data_from_rds_persistent_storage(form, param_config=config):
    """ Get data from RDS persistent storage"""
    engine, session = get_db_engine(param_config.PERSISTENT_DATABASE_URL)
    for row in session.query(
            form_tables(param_config=param_config)[form]
    ).yield_per(1000).enable_eagerloads(False):
        yield row.__dict__['data']


def subscribe_to_sqs(sqs_endpoint, sqs_queue_name):
    """ Subscribes to an sqs_enpoint with the sqs_queue_name"""
    logging.info("Connecting to SQS")
    region_name = "eu-west-1"

    if sqs_endpoint == 'DEFAULT':
        sqs_client = boto3.client('sqs', region_name=region_name)
    else:
        sqs_client = boto3.client('sqs', region_name=region_name,
                                  endpoint_url=sqs_endpoint)
    #sts_client = boto3.client('sts', region_name=region_name)

    logging.info("Getting SQS url")
    try:
        queue_url = sqs_client.get_queue_url(
            QueueName=sqs_queue_name,
        )['QueueUrl']
        logging.info("Subscribed to %s.", queue_url)
    except ClientError as e:
        print(e)
        print("sqs_queue_name", sqs_queue_name)
        #logging.info("Creating Queue")
        response = sqs_client.create_queue(
            QueueName=sqs_queue_name
        )
        queue_url = sqs_client.get_queue_url(
            QueueName=sqs_queue_name
        )['QueueUrl']
        logging.info("Subscribed to %s.", queue_url)
    return sqs_client, queue_url


def groupify(data):
    """
    Takes a dict with groups identified by ./ and turns it into a nested dict
    """
    new = {}
    for key in data.keys():
        if "./" in key:
            group, field = key.split("./")
            if group not in new:
                new[group] = {}
            new[group][field] = data[key]
        else:
            new[key] = data[key]

    return new


def submit_data_to_aggregate(data, form_id, aggregate_config):
    """ Submits data to aggregate """
    data.pop("meta/instanceID", None)
    data.pop("SubmissionDate", None)
    grouped_json = groupify(data)
    grouped_json["@id"] = form_id
    result = bf.etree(grouped_json, root=Element(form_id))
    aggregate_user = aggregate_config.get('aggregate_username', None)
    
    aggregate_password = aggregate_config.get('aggregate_password', None)
    auth = HTTPDigestAuth(aggregate_user, aggregate_password)
    aggregate_url = aggregate_config.get('aggregate_url', None)
    r = requests.post(aggregate_url + "/submission", auth=auth,
                      files={
                        "xml_submission_file":  ("tmp.xml", tostring(result), "text/xml")
                      })
    logging.info("Aggregate submission status code: " + str(r.status_code))
    return r.status_code


def read_csv(file_path):
    """
    Reads csvfile and returns list of rows

    Args:
        file_path: path of file to read

    Returns:
        rows(list): list of rows
    """

    with open(file_path, "r", encoding='utf-8', errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def create_topic_list(alert, locations, country_config=config.country_config):
    """
    Assemble the appropriate topic ID list for a given alert. Make sure the
    topic list includes all appropriate location levels from clinic to whole
    country.

    So for an alert with reason "rea_1", in country with prefix "null", from
    clinic "4" in district "3" in region "2" in country "1", we get a topic
    list that looks like:
        ['null-rea_1-4', 'null-rea_1-3', 'null-rea_1-2',
         'null-rea_1-1', 'null-allDis-4', 'null-allDis-3',
         'null-allDis-2', 'null-allDis-1']

    """

    prefix = [country_config["messaging_topic_prefix"]]
    reason = [alert["variables"]["alert_reason"], 'allDis']
    locs = [alert["clinic"], alert["region"], 1]

    # The district isn't stored in the alert model, so calulate it as the
    # parent of the clinic.
    district = locations[alert["clinic"]].parent_location
    if (district != alert["region"]):
        locs.append(district)

    combinations = itertools.product(prefix, locs, reason)

    topics = []
    for comb in combinations:
        topics.append(str(comb[0]) + "-" + str(comb[1]) + "-" + str(comb[2]))

    logging.debug("Sending alert to topic list: {}".format(topics))

    return topics


def send_alert(alert_id, alert, variables, locations, param_config=config):
    """
    Assemble the alert message and send it using the hermes API

    We need to send alerts to four topics to cover all the different possible
    subscriptions.

    There are:
    1-allDis for all locations and all diseases
    1-alert.reason for all locations and the specific disease
    alert.region-allDis for specific region and all diseases
    alert.region-alert.reason for specific region and specific disease


    Args:
        alert: the alert to we need to send a message about
        variables: dict with variables
        locations: dict with locations
    """
    if alert["date"] > datetime.now() - timedelta(days=7):
        # List the possible strings that construct an alert sms message
        district = ""
        if alert["district"]:
            district = locations[alert["district"]].name

        # To display date-times
        def tostr(date):
            try:
                return parse(date).strftime("%H:%M %d %b %Y")
            except AttributeError:
                return "Not available"  # Catch if date not a date type

        # Assemble the data to be shown in the messsage
        data = {
            "date": alert["date"].strftime("%d %b %Y"),
            "received": tostr(alert["variables"].get('alert_received')),
            "submitted": tostr(alert["variables"].get('alert_submitted')),
            "clinic": locations[alert["clinic"]].name,
            "district": district,
            "region": locations[alert["region"]].name,
            "uuid": alert["uuid"],
            "alert_id": alert_id,
            "reason": variables[alert["variables"]["alert_reason"]].name
        }
        data = {**alert["variables"], **data}

        # Get the message template to use
        template = variables[alert["variables"]['alert_reason']].alert_message
        if not template:
            template = "case"  # default to case message template

        # Create the alert messages using the Jinja2 templates
        text_template = get_env(param_config).get_template('alerts/{}/text'.format(template))
        text_message = text_template.render(data=data)

        sms_template = get_env(param_config).get_template('alerts/{}/sms'.format(template))
        sms_message = sms_template.render(data=data)

        html_template = get_env(param_config).get_template('alerts/{}/html'.format(template))
        html_message = html_template.render(data=data)

        # Structure and send the hermes request
        data = {
            "from": param_config.country_config['messaging_sender'],
            "topics": create_topic_list(alert, locations,
                                        country_config=config.country_config),
            "id": alert_id,
            "message": text_message,
            "sms-message": sms_message,
            "html-message": html_message,
            "subject": "Public Health Surveillance Alerts: #" + alert_id,
            "medium": ['email', 'sms']
        }
        logging.info("CREATED ALERT {}".format(data['message']))

        if not param_config.country_config["messaging_silent"]:
            libs.hermes('/publish', 'PUT', data,
                        config=param_config)

