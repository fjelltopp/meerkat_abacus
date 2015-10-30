"""
Functionality to turn raw data into codes
"""
from dateutil import parser

import meerkat_abacus.model as model
from meerkat_abacus.aggregation.variable import Variable

def get_variables(session):
    """
    get variables out of db turn them into Variable classes

    Args:
    session: db-session

    Returns:
    variables: dict of id:Variable
    """
    result = session.query(model.AggregationVariables)
    variables ={}
    for row in result:
        variables[row.id] = Variable(row)
    return variables


def to_code(row, variables, locations, date_column, table_name):
    """
    Takes a row and transforms it into a data row

    Args;
    row: row of raw data
    variables: dict of variables to check
    locations: list of locations
    """
    locations, locations_by_deviceid, regions, districts = locations
    clinic_id = locations_by_deviceid[row["deviceid"]]
    new_record = model.Data(
        date=parser.parse(row[date_column]),
        uuid=row["meta/instanceID"],
        clinic=clinic_id,
        clinic_type=locations[clinic_id].clinic_type,
        country=1,
        geolocation=locations[clinic_id].geolocation)
    if locations[clinic_id].parent_location in districts:
        new_record.district = locations[clinic_id].parent_location
        new_record.region = (
            locations[locations[clinic_id].parent_location].parent_location)
    elif locations[clinic_id].parent_location in regions:
        new_record.district = None
        new_record.region = locations[clinic_id].parent_location
    variable_json = {}
    for v in variables.keys():
        if table_name == variables[v].variable.form:
            test_outcome = variables[v].test(row)
            if test_outcome:
                variable_json[int(v)] = test_outcome
        new_record.variables = variable_json
    return new_record
