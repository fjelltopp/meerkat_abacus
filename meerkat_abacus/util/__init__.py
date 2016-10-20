"""
Various utility functions for meerkat abacus
"""
import csv, requests, json, itertools, logging
from datetime import datetime, timedelta

from meerkat_abacus.model import Locations, AggregationVariables, Devices
from meerkat_abacus.config import country_config
import meerkat_abacus.config as config


def epi_week_start_date(year, epi_config=country_config["epi_week"]):
    """
    Get the first day of epi week 1
    
    if epi_config==international epi_week 1 starts on the 1st of January
    
    if epi_config== day:X then the first epi_week start on the first weekday X after 1st of January
    X=0 is Sunday
    
    Args: 
        year: year
        epi_config: how epi-weeks are calculated
    Returns:
        start_date: date of start of epi week 1
    """
    if epi_config == "international":
        return datetime(year, 1, 1)
    else:
        day_of_week = int(epi_config.split(":")[1])
        first_of_year = datetime(year, 1, 1)
        f_day_of_week = first_of_year.weekday()
        adjustment = day_of_week - f_day_of_week
        if adjustment < 0:
            adjustment = 7 + adjustment
        return first_of_year + timedelta(days=adjustment)

    
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
    Transforms key in row to a list. We split on semicolons if they exist in the string,
    otherwise we use commas.
    
    Args:
        row: row of data
        key: key for the field we want
    Reutrns:
        row: modified row
    """
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
    regions, districts = get_regions_districts(session)

    devices = get_device_tags(session)
    return (locations, locations_by_deviceid, regions, districts, devices)


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
    
def get_regions_districts(session):
    """
    get list of ids for regions and districts

    Args:
        session: db session

    Returns:
        regions_district(tuple): (regions,districts)
    """
    locations = get_locations(session)
    regions = []
    districts = []
    for l in locations.keys():
        if locations[l].parent_location == 1:
            regions.append(l)
    for l in locations.keys():
        if locations[l].parent_location in regions:
            districts.append(l)
    return (regions, districts)


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
        start_date_by_deviceid[l] = locations[locations_by_deviceid[l]].start_date
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


def write_csv(rows, file_path):
    """
    Writes rows to csvfile

    Args:
        rows: list of dicts with data
        file_path: path to write file to
    """
    with open(file_path, "w", encoding='utf-8') as f:
        columns = sorted(list(rows[0]))
        out = csv.DictWriter(f, columns)
        out.writeheader()
        for row in rows:
            out.writerow(row)


def read_csv(file_path):
    """
    Reads csvfile and returns list of rows
    
    Args:
        file_path: path of file to read

    Returns:
        rows(list): list of rows
    """
    with open(file_path, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def refine_hermes_topics(topics):
    """
    We don't want mass emails to be sent from the dev environment, but we do want the ability to test.

    This function takes a list of hermes topics, and if we are in the development/testing 
    environment (determined by config "hermes_dev") this function strips them back to only those topics
    in the config variable "hermes_dev_topics".

    Args:
        topics ([str]) A list of topic ids that a message is initially intended to be published to.

    Returns:
        [str] A refined list of topic ids containing only those topics from config "hermes_dev_topics",
        if config "hermes_dev" == 1. 
    """

    #Make topics a copied (don't edit original) list if it isn't already one.
    topics = list([topics]) if not isinstance( topics, list ) else list(topics)

    logging.warning( "Initial topics: " + str( topics ) )

    #If in development/testing environment, remove topics that aren't pre-specified as allowed.
    if config.hermes_dev:
        for t in range( len(topics)-1, -1, -1 ):
            if topics[t] not in config.hermes_dev_topics:
                del topics[t]

    logging.warning( "Refined topics: " + str( topics ) )

    return topics

def hermes(url, method, data=None):
    """
    Makes a Hermes API request

    Args: 
       url: hermes url to send the request to
       method: post/get http method
       data: data to send
    """

    #If we are in the dev envirnoment only allow publishing to specially selected topics. 
    if data.get('topics', []):

        topics = refine_hermes_topics( data.get('topics', []) )
        #Return a error message if we have tried to publish a mass email from the dev envirnoment. 
        if not topics:
            return {"message": "No topics to publish to, perhaps because system is in hermes dev mode."}
        else:
            data['topics'] = topics

    #Add the API key and turn into JSON.
    data["api_key"] = config.hermes_api_key

    try:
        url = config.hermes_api_root + "/" + url
        headers = {'content-type': 'application/json'}
        r = requests.request(method, url, json=data, headers=headers)

    except Exception as e:
        logging.warning( "HERMES REQUEST FAILED: " + str(e) )

    output = ""

    try:
        output = r.json()
    except Exception as e:
        logging.warning( "HERMES REQUEST FAILED TO CONVERT TO JSON: " + str(e) )

    return output


def create_topic_list( alert, locations ):
    """
    Assemble the appropriate topic ID list for a given alert.
    Make sure the topic list includes all appropriate location levels from clinic to whole country.

    So for an alert with reason "rea_1", in country with prefix "null", from clinic "4" in district "3"
    in region "2" in country "1", we get a topic list that looks like:
        ['null-rea_1-4', 'null-rea_1-3', 'null-rea_1-2',
         'null-rea_1-1', 'null-allDis-4', 'null-allDis-3',
         'null-allDis-2', 'null-allDis-1']

    """ 

    prefix = [country_config["messaging_topic_prefix"]]
    reason = [alert.variables["alert_reason"], 'allDis']   
    locs = [alert.clinic, alert.region, 1]

    #The district isn't stored in the alert model, so calulate it as the parent of the clinic.
    district = locations[alert.clinic].parent_location
    if( district != alert.region ):
        locs.append( district )
    
    combinations = itertools.product( prefix, locs, reason )

    topics = []
    for comb in combinations:
        topics.append(str(comb[0]) + "-" + str(comb[1]) + "-" + str(comb[2]) )

    logging.warning( "Sending alert to topic list: {}".format(topics) )

    return topics

def send_alert(alert_id, alert, variables, locations):
    """
    Assemble the alert message and send it using the hermes API

    We need to send alerts to four topics to cover all the different possible subscriptions. 
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
    if alert.date > country_config['messaging_start_date']:

        alert_info = ("Alert: " + variables[alert.variables["alert_reason"]].name + "\n"
                      "Date: " + alert.date.strftime("%d %b %Y") + "\n"
                      "Clinic: " + locations[alert.clinic].name + "\n"
                      "Region: " + locations[alert.region].name + "\n"
                      "Gender: " + alert.variables["alert_gender"].title() + "\n"
                      "Age: " + alert.variables["alert_age"] + "\n"
                      "Alert ID: " + alert_id + "\n" )

        message = (alert_info +
                   "Patient ID: " + alert.uuid + "\n\n"
                   "To unsubscribe from <<country>> public health surveillance notifications "
                   "please copy and paste the following url into your browser's address bar:\n"
                   "https://hermes.aws.emro.info/unsubscribe/<<id>>\n\n" )

        sms_message = ("<<country>> Public Health Surveillance Alert:\n\n" + alert_info)

        html_message = ("<table style='border:none; margin-left: 20px;'>"
                        "<tr><td><b>Alert:</b></td><td>" + variables[alert.variables["alert_reason"]].name + "</td></tr>"
                        "<tr><td><b>Date:</b></td><td>" + alert.date.strftime("%d %b %Y") + "</td></tr>"
                        "<tr><td><b>Clinic:</b></td><td>" + locations[alert.clinic].name + "</td></tr>"
                        "<tr><td><b>Region:</b></td><td>" + locations[alert.region].name + "</td></tr>"
                        "<tr style='height:10px'></tr>"
                        "<tr><td><b>Patient ID:</b></td><td>" + alert.uuid + "</td></tr>" 
                        "<tr><td><b>Gender:</b></td><td>" + alert.variables["alert_gender"].title() + "</td></tr>"
                        "<tr><td><b>Age:</b></td><td>" + alert.variables["alert_age"] + "</td></tr>"
                        "<tr style='height:10px'></tr>"
                        "<tr><td><b>Alert ID:</b></td><td>" + alert_id + "</td></tr></table>"
                        "<p>To unsubscribe from <<country>> public health surveillance notifications "
                        "please <a href='https://hermes.aws.emro.info/unsubscribe/<<id>>' target='_blank'>"
                        "click here</a>.</p>")      

        data = {
            "from": country_config['messaging_sender'],
            "topics": create_topic_list( alert, locations ),
            "id": alert_id,
            "message": message,
            "sms-message": sms_message,
            "html-message": html_message,
            "subject": "Public Health Surveillance Alerts: #" + alert_id,
            "medium": ['email', 'sms']
        }
        hermes('publish', 'PUT', data)
        #TODO: Add some error handling here!
