"""
Functionality to turn raw data into codes
"""
from dateutil import parser
from datetime import datetime
import meerkat_abacus.model as model
from meerkat_abacus.codes.variable import Variable

def get_variables(session):
    """
    Get the variables out of the db and turn them into Variable classes.

    To speed up the next step of the process we group the variables by calculation_group. 

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
        variables.setdefault(row.type, {})
        variables[row.type].setdefault(group, {})
        variables[row.type][group][row.id] = Variable(row)
    return variables


def to_code(row, variables, locations, data_type, alert_data):
    """
    Takes a row and transforms it into a data row

    We iterate through each variable and add the variable_id: test_outcome to the 
    data.variable json dictionary if test_outcome is True. 

    To speed up this process we have divded the variables into groups where only one variable
    can be apply to the given record. As soon as we find one of these variables, we don't test
    the rest of the variables in the same group. 

    Args;
        row: row of raw data
        variables: dict of variables to check
        locations: list of locations
        date_column: which column from the row determines the date
        table_name: the name of the table/from the row comes from
        alert_data: a dictionary of name:column pairs. For each alert we return the value of row[column] as name. 
    return:
        new_record(model.Data): Data record
        alert(model.Alerts): Alert record if created
    """
    locations, locations_by_deviceid, regions, districts = locations
    clinic_id = locations_by_deviceid.get(row["deviceid"], None)
    if not clinic_id:
        return (None, None, None)
    ret_location = {
        "clinic":clinic_id,
        "clinic_type":locations[clinic_id].clinic_type,
        "country":1,
        "geolocation":locations[clinic_id].geolocation
    }

    if locations[clinic_id].parent_location in districts:
        ret_location["district"] = locations[clinic_id].parent_location
        ret_location["region"] = (
            locations[locations[clinic_id].parent_location].parent_location)
    elif locations[clinic_id].parent_location in regions:
        ret_location["district"] = None
        ret_location["region"] = locations[clinic_id].parent_location
    variable_json = {}
    alert = None
    for group in variables[data_type].keys():
        #All variables in group have same secondary conndition, so only check once
        for v in variables[data_type][group]:
            test_outcome = variables[data_type][group][v].test_type(row, None)
            if test_outcome:
                variable_json[v] = test_outcome
                if variables[data_type][group][v].variable.alert:
                    variable_json["alert"] = 1
                    variable_json["alert_reason"] = variables[data_type][group][v].variable.id
                    for data_var in alert_data.keys():
                        variable_json["alert_"+data_var] = row[alert_data[data_var]]
                break # We break out of the current group as all variables in a group are mutually exclusive
    return (variable_json, ret_location)
















    
                    # # If the variable we just found as a match is a variable we should create an alert for
                    # data_alert = {}
                    # for data_var in alert_data.keys():
                    #     data_alert[data_var] = row[alert_data[data_var]]
                    # alert = model.Alerts(
                    #     uuids=row["meta/instanceID"],
                    #     clinic=clinic_id,
                    #     region=new_record.region, 
                    #     reason=v,
                    #     data=data_alert,
                    #     date=date)
