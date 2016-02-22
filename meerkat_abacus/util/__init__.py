"""
Various utility functions
"""
import csv, requests, json
from datetime import datetime, timedelta
from meerkat_abacus.model import Locations, LinkDefinitions, AggregationVariables
from meerkat_abacus.config import country_config, hermes_api_root, hermes_api_key


def epi_week_start_date(year, epi_config=country_config["epi_week"]):
    """
    Get the first day of epi week 1
    
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
    get a links dict

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
    


def add_new_data(form, data, session):
    """
    adds rows in data that has a uuid not already in the form

    Args:
        form: form to add to
        data: data to potentially be added
        session: db session

    Returns:
        new_rows(list): a list of rows added
    """
    result = session.query(form.uuid)
    uuids = []
    for r in result:
        uuids.append(r.uuid)
    new_rows = []
    for row in data:
        if row["meta/instanceID"] not in uuids:
            session.add(form(uuid=row["meta/instanceID"], data=row))
            new_rows.append(row)
    session.commit()
    return new_rows

def all_location_data(session):
    """
    get all location data

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
    get variables out of db turn them into Variable classes

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
    get a dict with deviceid: locatino:id

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
    get a location dict

    Args:
        session: db session

    Returns:
        locations(dict) : id:location dict
    """
    result = session.query(Locations)
    locations = {}
    for row in result:
        locations[row.id]=row
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
    f = open(file_path, "w", encoding='utf-8')
    columns = list(rows[0])
    out = csv.DictWriter(f, columns)
    out.writeheader()
    for row in rows:
        out.writerow(row)
    f.close()


def read_csv(file_path):
    """
    Reads csvfile and returns list of rows
    
    Args:
        file_path: path of file to read

    Returns:
        rows(list): list of rows
    """
    f = open(file_path, "r", encoding='utf-8')
    reader = csv.DictReader(f)
    rows = []
    for row in reader:
        rows.append(row)
    f.close()
    return rows

def hermes(url, method, data={}):
    """Makes a Hermes API request"""

    #Add the API key and turn into JSON.
    data["api_key"] = hermes_api_key

    #Assemble the other request params.
    url = hermes_api_root + url 
    headers = {'content-type' : 'application/json'}
    
    #Make the request and handle the response.
    r = requests.request( method, url, json=data, headers=headers)
    return r.json()   

def send_alert( alert, variables, locations ):

    if alert.date > country_config['messaging_start_date']:

        topics=[
            country_config["messaging_topic_prefix"] + "-1-allDis",
            country_config["messaging_topic_prefix"] + "-1-" + alert.reason,
            country_config["messaging_topic_prefix"] + "-" + str(alert.region) + "-allDis",
            country_config["messaging_topic_prefix"] + "-" + str(alert.region) + "-" + alert.reason      
        ]

        alert_info = ( "Alert: " + variables[alert.reason].name + "\n"
                       "Date: " + alert.date.strftime("%d %b %Y") + "\n"
                       "Clinic: " + locations[alert.clinic].name + "\n"
                       "Region: " + locations[alert.region].name + "\n\n"
                       "Patient ID: " + alert.uuids + "\n" 
                       "Gender: " + alert.data["gender"].title() + "\n"
                       "Age: " + alert.data["age"] + "\n\n"
                       "Alert ID: " + alert.id + "\n\n" )

        message = ( "Dear <<first_name>> <<last_name>>,\n\n"
                    "There has been an alert that we think you'll be interested in. "
                    "Here are the details:\n\n" + alert_info +
                    "If you would like to unsubscribe from Meerkat Health Surveillance notifications "
                    "please copy and post the following url into your browser's address bar:\n"
                    "https://hermes.aws.emro.info/unsubscribe/<<id>>\n\n"
                    "Best wishes,\nThe Meerkat Health Surveillance team" )

        sms_message = ( "An alert from Meerkat Health Surveillance:\n\n" + alert_info )

        html_message = ( "<p>Dear <<first_name>> <<last_name>>,</p>"
                         "<p>There has been an alert that we think you'll be interested in. "
                         "Here are the details:</p><table style='border:none; margin-left: 20px;'>"
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
                         "<p>If you would like to unsubscribe from Meerkat Health Surveillance notifications "
                         "please <a href='https://hermes.aws.emro.info/unsubscribe/<<id>>' target='_blank'>"
                         "click here</a>.</p>"
                         "<p>Best wishes,<br>The Meerkat Health Surveillance team</p>" )        

        data={
            "from": country_config['messaging_sender'],
            "topics": topics,
            "id": alert.id,
            "message": message,
            "sms-message": sms_message,
            "html-message": html_message,
            "subject": "Meerkat Health Surveillance Alerts: #" + alert.id,
            "medium": ['email','sms']
        }

        hermes('/publish', 'PUT', data)
     
