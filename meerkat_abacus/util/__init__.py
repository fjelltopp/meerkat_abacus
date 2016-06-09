"""
Various utility functions for meerkat abacus
"""
import csv, requests, json, itertools, logging
from datetime import datetime, timedelta
from meerkat_abacus.model import Locations, LinkDefinitions, AggregationVariables
from meerkat_abacus.config import country_config, hermes_api_root, hermes_api_key


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

    return (locations, locations_by_deviceid, regions, districts)


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
        rows = []
        for row in reader:
            yield row
    #         rows.append(row)
    # return rows


def hermes(url, method, data=None):
    """
    Makes a Hermes API request

    Args: 
       url: hermes url to send the request to
       method: post/get http method
       data: data to send
    """
    if country_config["messaging_silent"]:
        return {"message": "Abacus is in silent mode"}
    data["api_key"] = hermes_api_key
    url = hermes_api_root + url
    headers = {'content-type': 'application/json'}
    r = requests.request(method, url, json=data, headers=headers)
    return r.json()


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
    logging.warning( str(alert ))
    prefix = [country_config["messaging_topic_prefix"]]
    reason = [alert.reason, 'allDis']   
    locs = [alert.clinic, alert.region, 1]

    #The district isn't stored in the alert model, so calulate it as the parent of the clinic.
    district = locations[alert.clinic].parent_location
    if( district != alert.region ):
        locs.append( district )
    
    combinations = itertools.product( prefix, locs, reason )

    topics = []
    for comb in combinations:
        topics.append(str(comb[0]) + "-" + str(comb[1]) + "-" + str(comb[2]) )

    logging.warning( "Sending alert to topic list: " + str(topics) )

    return topics

def send_alert(alert, variables, locations):
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

        alert_info = ("Alert: " + variables[alert.reason].name + "\n"
                      "Date: " + alert.date.strftime("%d %b %Y") + "\n"
                      "Clinic: " + locations[alert.clinic].name + "\n"
                      "Region: " + locations[alert.region].name + "\n\n"
                      "Patient ID: " + alert.uuids + "\n"
                      "Gender: " + alert.data["gender"].title() + "\n"
                      "Age: " + alert.data["age"] + "\n\n"
                      "Alert ID: " + alert.id + "\n\n" )

        message = (alert_info +
                   "To unsubscribe from <<country>> public health surveillance notifications "
                   "please copy and paste the following url into your browser's address bar:\n"
                   "https://hermes.aws.emro.info/unsubscribe/<<id>>\n\n" )

        sms_message = ("A public health surveillance alert from <<country>>:\n\n" + alert_info)

        html_message = ("<table style='border:none; margin-left: 20px;'>"
                        "<tr><td><b>Alert:</b></td><td>" + variables[alert.reason].name + "</td></tr>"
                        "<tr><td><b>Date:</b></td><td>" + alert.date.strftime("%d %b %Y") + "</td></tr>"
                        "<tr><td><b>Clinic:</b></td><td>" + locations[alert.clinic].name + "</td></tr>"
                        "<tr><td><b>Region:</b></td><td>" + locations[alert.region].name + "</td></tr>"
                        "<tr style='height:10px'></tr>"
                        "<tr><td><b>Patient ID:</b></td><td>" + alert.uuids + "</td></tr>" 
                        "<tr><td><b>Gender:</b></td><td>" + alert.data["gender"].title() + "</td></tr>"
                        "<tr><td><b>Age:</b></td><td>" + alert.data["age"] + "</td></tr>"
                        "<tr style='height:10px'></tr>"
                        "<tr><td><b>Alert ID:</b></td><td>" + alert.id + "</td></tr></table>"
                        "<p>To unsubscribe from <<country>> public health surveillance notifications "
                        "please <a href='https://hermes.aws.emro.info/unsubscribe/<<id>>' target='_blank'>"
                        "click here</a>.</p>")        

        data = {
            "from": country_config['messaging_sender'],
            "topics": create_topic_list( alert, locations ),
            "id": alert.id,
            "message": message,
            "sms-message": sms_message,
            "html-message": html_message,
            "subject": "Public Health Surveillance Alerts: #" + alert.id,
            "medium": ['email', 'sms']
        }
        hermes('/publish', 'PUT', data)
        #Add some error handling here!
