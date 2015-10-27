"""
Various utility functions
"""
import csv

from meerkat_abacus.model import Locations


def get_deviceids(session, case_report=False):
    """
    Returns a list of deviceids

    Args:
    session: SQLAlchemy session
    case_report: flag to only get deviceids from case 
                 reporing clinics

    Returns:
    list_of_deviceids
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
