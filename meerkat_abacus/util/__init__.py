"""
Various utility functions for meerkat abacus
"""
from meerkat_abacus.model import Locations, AggregationVariables, Devices
from meerkat_abacus.config import country_config
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import meerkat_libs as libs
import meerkat_abacus.config as config
import itertools
import logging
import csv
import boto3
from botocore.exceptions import ClientError
from xmljson import badgerfish as bf
from lxml.html import Element, tostring
import requests
from requests.auth import HTTPDigestAuth


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


def epi_week(date):
    """
    calculate epi week

    Args:
        date
    Returns epi_week
    """
    start_date = epi_week_start_date(date.year)
    year = start_date.year
    # If the date is before the start date, include in week 1.
    if date < start_date:
        return year, 1
    return year, (date - start_date).days // 7 + 1


def get_db_engine():
    """
    Returns a db engine and session
    """
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session


def epi_week_start_date(year, epi_config=country_config["epi_week"]):
    """
    Get the first day of epi week 1

    if epi_config==international epi_week 1 starts on the 1st of January

    if epi_config== day:X then the first epi_week start on the first weekday
    X after 1st of January
    X=0 is Sunday

    Args:
        year: year
        epi_config: how epi-weeks are calculated
    Returns:
        start_date: date of start of epi week 1
    """
    if epi_config == "international":
        return datetime(year, 1, 1)
    elif "day" in epi_config:
        day_of_week = int(epi_config.split(":")[1])
        first_of_year = datetime(year, 1, 1)
        f_day_of_week = first_of_year.weekday()
        adjustment = day_of_week - f_day_of_week
        if adjustment < 0:
            adjustment = 7 + adjustment
        return first_of_year + timedelta(days=adjustment)
    else:
        return epi_config.get(year, datetime(year, 1, 1))


def get_link_definitions(session):
    """
    gets all the link definitions from the db

    Args:
        session: db session

    Returns:
        links(dict) : id:link
    """
    result = session.query(LinkDefinitions)
    links = {}
    for row in result:
        links[row.id] = row
    return links


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
    #    if row.area is not None:
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


def read_csv_filename(filename, config=None):
    """ Read a csv file from the filename"""
    file_path = config.data_directory + filename + ".csv"
    for row in read_csv(file_path):
        yield row


def subscribe_to_sqs(sqs_endpoint, sqs_queue_name):
    """ Subscribes to an sqs_enpoint with the sqs_queue_name"""
    logging.info("Connecting to SQS")
    region_name = "eu-west-1"

    if sqs_endpoint == 'DEFAULT':
        sqs_client = boto3.client('sqs', region_name=region_name)
    else:
        sqs_client = boto3.client('sqs', region_name=region_name,
                                  endpoint_url=sqs_endpoint)
    sts_client = boto3.client('sts', region_name=region_name)

    logging.info("Getting SQS url")
    try:
        queue_url = sqs_client.get_queue_url(
            QueueName=sqs_queue_name,
            QueueOwnerAWSAccountId=""
        )['QueueUrl']
        logging.info("Subscribed to %s.", queue_url)
    except ClientError as e:
        print(e)
        logging.info("Creating Queue")
        response = sqs_client.create_queue(
            QueueName=config.sqs_queue
        )
        queue_url = sqs_client.get_queue_url(
            QueueName=sqs_queue_name,
            QueueOwnerAWSAccountId=sts_client.get_caller_identity()["Account"]
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


def submit_data_to_aggregate(data, form_id, config):
    """ Submits data to aggregate """
    data.pop("meta/instanceID", None)
    data.pop("SubmissionDate", None)
    grouped_json = groupify(data)
    grouped_json["@id"] = form_id
    result = bf.etree(grouped_json, root=Element(form_id))
    aggregate_user = config.aggregate_username
    
    aggregate_password = config.aggregate_password
    auth = HTTPDigestAuth(aggregate_user, aggregate_password)
    aggregate_url = config.aggregate_url
    r = requests.post(aggregate_url + "/submission", auth=auth,
                      files={
                          "xml_submission_file":  ("tmp.xml", tostring(result), "text/xml")
                      })
    logging.info("Aggregate submission status code: " + r.status_code)
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

def create_topic_list(alert, locations):
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
    reason = [alert.variables["alert_reason"], 'allDis']
    locs = [alert.clinic, alert.region, 1]

    # The district isn't stored in the alert model, so calulate it as the
    # parent of the clinic.
    district = locations[alert.clinic].parent_location
    if(district != alert.region):
        locs.append(district)

    combinations = itertools.product(prefix, locs, reason)

    topics = []
    for comb in combinations:
        topics.append(str(comb[0]) + "-" + str(comb[1]) + "-" + str(comb[2]))

    logging.debug("Sending alert to topic list: {}".format(topics))

    return topics


def send_alert(alert_id, alert, variables, locations):
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
    if alert.date > datetime.now() - timedelta(days=7):

        # List the possible strings that construct an alert sms message
        district = ""
        if alert.district:
            district = locations[alert.district].name

        text_strings = {
            'date': "Date: " + alert.date.strftime("%d %b %Y") + "\n",
            'clinic': "Clinic: " + locations[alert.clinic].name + "\n",
            'district': "District: " + district + "\n",
            'region': "Region: " + locations[alert.region].name + "\n",
            'patient': "Patient ID: " + alert.uuid + "\n",
            'age': "Age: " + str(alert.variables["alert_age"]) + "\n",
            'id': "Alert ID: " + alert_id + "\n",
            'reason': ("Alert: " +
                       variables[alert.variables["alert_reason"]].name + "\n"),
            'gender': ("Gender: " + alert.variables["alert_gender"].title() +
                       "\n"),
        }

        # List the possible strings that construct an alert email message
        html_strings = {
            'reason': ("<tr><td><b>Alert:</b></td><td>" +
                       variables[alert.variables["alert_reason"]].name +
                       "</td></tr>"),
            'date': ("<tr><td><b>Date:</b></td><td>" +
                     alert.date.strftime("%d %b %Y") + "</td></tr>"),
            'clinic': ("<tr><td><b>Clinic:</b></td><td>" +
                       locations[alert.clinic].name + "</td></tr>"),
            'district': ("<tr><td><b>District:</b></td><td>" +
                         district + "</td></tr>"),
            'region': ("<tr><td><b>Region:</b></td><td>" +
                       locations[alert.region].name + "</td></tr>"),
            'patient': ("<tr><td><b>Patient ID:</b></td><td>" +
                        alert.uuid + "</td></tr>"),
            'gender': ("<tr><td><b>Gender:</b></td><td>" +
                       alert.variables["alert_gender"].title() + "</td></tr>"),
            'age': ("<tr><td><b>Age:</b></td><td>" +
                    str(alert.variables["alert_age"]) + "</td></tr>"),
            'id': ("<tr><td><b>Alert ID:</b></td><td>" + alert_id +
                   "</td></tr>"),
            'breaker': "<tr style='height:10px'></tr>"
        }

        # Get which sms strings to be used and in which order from the country
        # config.
        sms_data = country_config.get(
            'alert_sms_content',
            ['reason', 'date', 'clinic', 'region', 'gender', 'age', 'id']
        )

        # Assemble alert info for sms message from configs.
        sms_alert_info = ""
        for item in sms_data:
            sms_alert_info += text_strings.get(item, "")

        # Get which text strings to be used and in which order from the country
        # config.
        text_data = country_config.get(
            'alert_text_content',
            ['reason', 'date', 'clinic', 'region',
                'patient', 'gender', 'age', 'id']
        )

        # Assemble the alert info for a plain text email message.
        alert_info = ""
        for item in text_data:
            alert_info += text_strings.get(item, "")

        # Get which sms strings to be used and in which order from the country
        # config.
        html_data = country_config.get(
            'alert_email_content',
            ['reason', 'date', 'clinic', 'region', 'breaker',
                'patient', 'gender', 'age', 'breaker', 'id']
        )

        # Assemble alert info for email message from configs.
        html_alert_info = "<table style='border:none; margin-left: 20px;'>"
        for item in html_data:
            html_alert_info += html_strings.get(item, "")
        html_alert_info += "</table>"

        # Add to the alert info any other necessary information and store for
        # sending to hermes.
        message = (
            alert_info + "To unsubscribe from <<country>> public health "
            "surveillance notifications please copy and paste the following "
            "url into your browser's address bar:\n"
            "https://hermes.aws.emro.info/unsubscribe/<<id>>\n\n"
        )
        sms_message = (
            "<<country>> Public Health Surveillance Alert:\n\n" +
            sms_alert_info
        )
        html_message = (
            html_alert_info +
            "<p>To unsubscribe from <<country>> public health surveillance "
            "notifications please <a href='https://hermes.aws.emro.info/"
            "unsubscribe/<<id>>' target='_blank'>click here</a>.</p>"
        )

        # Structure and send the hermes request
        data = {
            "from": country_config['messaging_sender'],
            "topics": create_topic_list(alert, locations),
            "id": alert_id,
            "message": message,
            "sms-message": sms_message,
            "html-message": html_message,
            "subject": "Public Health Surveillance Alerts: #" + alert_id,
            "medium": ['email', 'sms']
        }

        logging.debug("CREATED ALERT {}".format(data['id']))

        libs.hermes('/publish', 'PUT', data)
        # TODO: Add some error handling here!
