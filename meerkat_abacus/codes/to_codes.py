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


def to_code(row, variables, locations, data_type, location_form, alert_data):
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
    clinic_id = locations_by_deviceid.get(row[location_form]["deviceid"], None)
    if not clinic_id:
        return (None, None)
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
    multiple_method = {"last": -1, "first": 0}
    for group in variables[data_type].keys():
        #All variables in group have same secondary conndition, so only check once
        for v in variables[data_type][group]:
            form = variables[data_type][group][v].variable.form
            if form in row and row[form]:
                if isinstance(row[form], list):
                    method = variables[data_type][group][v].variable.multiple_link
                    if method in ["last", "first"]:
                        data = row[form][multiple_method[method]]
                        test_outcome = variables[data_type][group][v].test_type(data)
                    elif method == "count":
                        test_outcome = len(row[form])
                    elif method == "any":
                        test_outcome = 0
                        for d in row[form]:
                            test_outcome = variables[data_type][group][v].test_type(d)
                            if test_outcome:
                                break
                    elif method == "all":
                        test_outcome = 1
                        for d in row[form]:
                            t_o = variables[data_type][group][v].test_type(d)
                            if not t_o:
                                test_outcome = 0
                                break
                else:
                    test_outcome = variables[data_type][group][v].test_type(row[form])
                    data = row[form]
                if test_outcome:
                    variable_json[v] = test_outcome
                    if variables[data_type][group][v].variable.alert:
                        variable_json["alert"] = 1
                        variable_json["alert_reason"] = variables[data_type][group][v].variable.id
                        for data_var in alert_data.keys():
                            variable_json["alert_"+data_var] = row[location_form][alert_data[data_var]]
                    break # We break out of the current group as all variables in a group are mutually exclusive
    return (variable_json, ret_location)

