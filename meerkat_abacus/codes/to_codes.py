"""
Functionality to turn raw data into codes
"""
from dateutil import parser

import meerkat_abacus.model as model
from meerkat_abacus.codes.variable import Variable

def get_variables(session):
    """
    get variables out of db turn them into Variable classes

    Args:
        session: db-session

    Returns:
        variables(dict): dict of id:Variable
    """
    result = session.query(model.AggregationVariables)
    variables = {}
    for row in result:
        group = row.calculation_group
        if not group:
            group = row.id
        variables.setdefault(row.form, {})
        variables[row.form].setdefault(group, {})
        variables[row.form][group][row.id] = Variable(row)
    return variables


def to_code(row, variables, locations, date_column, table_name, alert_data):
    """
    Takes a row and transforms it into a data row

    Args;
        row: row of raw data
        variables: dict of variables to check
        locations: list of locations
    return:
        new_record(model.Data): Data record
        alert(model.Alerts): Alert record if created
    """
    locations, locations_by_deviceid, regions, districts = locations
    clinic_id = locations_by_deviceid.get(row["deviceid"], None)
    if not clinic_id:
        return (None, None)
    date = parser.parse(row[date_column])
    new_record = model.Data(
        date=date,
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
    alert = None
    if table_name in variables.keys():
        for group in variables[table_name].keys():
            #All variables in group have same secondary conndition, so only check once
            first_variable = next(iter(variables[table_name][group].values()))
            if first_variable.secondary_condition(row):
                value = row.get(first_variable.column, "neppe")
                for v in variables[table_name][group]:
                    test_outcome = variables[table_name][group][v].test_type(row, value)
                    if test_outcome:
                        variable_json[v] = int(test_outcome)
                        if variables[table_name][group][v].variable.alert:
                            data_alert = {}
                            for data_var in alert_data.keys():
                                data_alert[data_var] = row[alert_data[data_var]]
                            alert = model.Alerts(
                                uuids=row["meta/instanceID"],
                                clinic=clinic_id,
                                reason=v,
                                data=data_alert,
                                date=date)
                        break
    new_record.variables = variable_json
    return (new_record, alert)


    
