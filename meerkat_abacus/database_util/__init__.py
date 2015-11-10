"""
Various utility functions
"""
import csv

from meerkat_abacus.model import Locations, LinkDefinitions

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
    f = open(file_path, "w")
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
