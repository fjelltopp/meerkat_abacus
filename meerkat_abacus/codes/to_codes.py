"""
Functionality to turn raw data into codes
"""
import meerkat_abacus.model as model
from meerkat_abacus.codes.variable import Variable


def get_variables(session, restrict=None):
    """
    Get the variables out of the db and turn them into Variable classes.

    To speed up the next step of the process we group the variables by calculation_group. 

    Args:
        session: db-session

    Returns:
        variables(dict): dict of id:Variable
    """
    if restrict:
        result = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.type == restrict)
    else:
        result = session.query(model.AggregationVariables)
    variables = {}

    variable_forms = {}
    variable_tests = {}
    variables_group = {}
    for row in result:
        group = row.calculation_group
        if not group:
            group = row.id
        variables.setdefault(row.type, {})
        variables[row.type].setdefault(group, {})
        variables[row.type][group][row.id] = Variable(row)
        variable_forms[row.id] = row.form
        variable_tests[row.id] = variables[row.type][group][row.id].test_type
        variables_group.setdefault(group, [])
        variables_group[group].append(row.id)

    return variables, variable_forms, variable_tests, variables_group


multiple_method = {"last": -1, "first": 0}


def to_code(row, variables, locations, data_type, location_form, alert_data,
            mul_forms):
    """
    Takes a row and transforms it into a data row

    We iterate through each variable and add the variable_id: test_outcome to the 
    data.variable json dictionary if test_outcome is True.

    To speed up this process we have divded the variables into groups where only one variable
    can be apply to the given record. As soon as we find one of these variables, we don't test
    the rest of the variables in the same group.

    Args:
        row: row of raw data
        variables: dict of variables to check
        locations: list of locations
        date_column: which column from the row determines the date
        table_name: the name of the table/from the row comes from
        alert_data: a dictionary of name:column pairs.
            For each alert we return the value of row[column] as name.
    return:
        new_record(model.Data): Data record
        alert(model.Alerts): Alert record if created
    """
    locations, locations_by_deviceid, regions, districts, devices = locations
    clinic_id = locations_by_deviceid.get(row[location_form]["deviceid"], None)
    if not clinic_id:
        return (None, None, None, None)
    ret_location = {
        "clinic": clinic_id,
        "clinic_type": locations[clinic_id].clinic_type,
        "case_type": locations[clinic_id].case_type,
        "tags": devices[row[location_form]["deviceid"]],
        "country": 1,
        "geolocation": locations[clinic_id].geolocation
    }
    variables, variable_forms, variable_tests, variables_group = variables

    if locations[clinic_id].parent_location in districts:
        ret_location["district"] = locations[clinic_id].parent_location
        ret_location["region"] = (
            locations[locations[clinic_id].parent_location].parent_location)
    elif locations[clinic_id].parent_location in regions:
        ret_location["district"] = None
        ret_location["region"] = locations[clinic_id].parent_location
    else:
        ret_location["district"] = None
        ret_location["region"] = None
    variable_json = {}
    categories = {}
    disregard = False
    for group in variables[data_type].keys():
        for v in variables_group[group]:
            form = variable_forms[v]
            datum = row.get(form, None)
            if datum:
                if form in mul_forms:
                    method = variables[data_type][group][
                        v].variable.multiple_link
                    if method in ["last", "first"]:
                        data = datum[multiple_method[method]]
                        test_outcome = variables[data_type][group][
                            v].test_type(data)
                    elif method == "count":
                        test_outcome = len(datum)
                    elif method == "any":
                        test_outcome = 0
                        for d in datum:
                            test_outcome = variables[data_type][group][
                                v].test_type(d)
                            if test_outcome:
                                break
                    elif method == "all":
                        test_outcome = 1
                        for d in datum:
                            t_o = variables[data_type][group][v].test_type(d)
                            if not t_o:
                                test_outcome = 0
                                break
                else:
                    test_outcome = variable_tests[v](datum)
                if test_outcome:
                    if test_outcome == 1:
                        test_outcome = 1
                    variable_json[v] = test_outcome
                    for cat in variables[data_type][group][v].variable.category:
                        categories[cat] = v
                    if variables[data_type][group][v].variable.alert:
                        if variables[data_type][group][
                                v].variable.alert_type == "individual":
                            variable_json["alert"] = 1
                            variable_json["alert_type"] = "individual"
                            variable_json["alert_reason"] = variables[
                                data_type][group][v].variable.id
                            for data_var in alert_data.keys():
                                variable_json["alert_" + data_var] = row[
                                    location_form][alert_data[data_var]]
                    if variables[data_type][group][v].variable.disregard:
                        disregard = True
                    break  # We break out of the current group as all variables in a group are mutually exclusive
    if disregard and variable_json.get("alert_type", None) != "individual":
        disregard = False
    return (variable_json, categories, ret_location, disregard)
