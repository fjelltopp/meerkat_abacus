"""
Functionality to turn raw data into codes
"""
import meerkat_abacus.model as model
from meerkat_abacus.codes.variable import Variable
from geoalchemy2.shape import from_shape, to_shape
from shapely.geometry import Point

def get_variables(session, restrict=None, match_on_type=None, match_on_form=None):
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

    match_variables = {}
    for row in result:
        group = row.calculation_group
        if not group:
            group = row.id_pk
        if match_on_form is not None and match_on_type is not None:
            if row.method =="match" and row.calculation_priority in ["", None] and row.form == match_on_form and row.type == match_on_type:
                col = row.db_column
                match_variables.setdefault(col, {})
                for value in row.condition.split(","):
                    match_variables[col].setdefault(value.strip(), [{}, {}])
                    match_variables[col][value][0][row.id] = 1
                    if row.alert and row.alert_type == "individual":
                        match_variables[col][value][0]["alert"] = 1
                        match_variables[col][value][0]["alert_reason"] = row.id
                        match_variables[col][value][0]["alert_type"] = "individual"
                    for c in row.category:
                        match_variables[col][value][1][c] = row.id
                    
            else:
                variables_group.setdefault(group, [])
                variables_group[group].append(row.id_pk)
                variables.setdefault(row.type, {})
                variables[row.type].setdefault(group, {})
                variables[row.type][group][row.id_pk] = Variable(row)
                variable_forms[row.id_pk] = row.form
                variable_tests[row.id_pk] = variables[row.type][
                    group][row.id_pk].test_type
        else:
            variables_group.setdefault(group, [])
            variables_group[group].append(row.id_pk)
            variables.setdefault(row.type, {})
            variables[row.type].setdefault(group, {})
            variables[row.type][group][row.id_pk] = Variable(row)
            variable_forms[row.id_pk] = row.form
            variable_tests[row.id_pk] = variables[row.type][
                group][row.id_pk].test_type


    return (variables, variable_forms, variable_tests,
            variables_group, match_variables)


multiple_method = {"last": -1, "first": 0}

# @profile
def to_code(row, variables, locations, data_type, location_form, alert_data,
            mul_forms, location):
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

    if "deviceid" in location:
        column = "deviceid"
        prefix = ""
        if ":" in location:
            splitted = location.split(":")
            column = location.split(":")[1]
            if len(splitted) == 3:
                prefix = location.split(":")[2]
        
        clinic_id = locations_by_deviceid.get(prefix + row[location_form][column],
                                              None)
        if not clinic_id:
            return (None, None, None, None)
        clinic_gps = None
        if locations[clinic_id].point_location is not None:
            clinic_gps = locations[clinic_id].point_location.desc
        ret_location = {
            "clinic": clinic_id,
            "clinic_type": locations[clinic_id].clinic_type,
            "case_type": locations[clinic_id].case_type,
            "tags": devices.get(row[location_form].get("deviceid", None), None),
            "country": 1,
            "geolocation": clinic_gps
        }
        if locations[clinic_id].parent_location in districts:
            ret_location["district"] = locations[clinic_id].parent_location
            ret_location["region"] = (
                locations[ret_location["district"]].parent_location)
            ret_location["zone"] = (
                 locations[ret_location["region"]].parent_location)
        elif locations[clinic_id].parent_location in regions:
            ret_location["district"] = None
            ret_location["region"] = locations[clinic_id].parent_location
            ret_location["zone"] = (
                 locations[ret_location["region"]].parent_location)
        else:
            ret_location["district"] = None
            ret_location["region"] = None
            ret_location["zone"] = None
        row[location_form]["clinic_type"] = locations[clinic_id].clinic_type
        row[location_form]["service_provider"] = locations[clinic_id].service_provider
        
    elif "in_geometry" in location:
        fields = location.split("$")[1].split(",")
        try:
            point = Point(float(row[location_form][fields[0]]),
                          float(row[location_form][fields[1]]))
            found = False
            for loc in locations.values():
                if loc.level == "district":
                    if loc.area is not None and to_shape(loc.area).contains(point):
                        ret_location = {
                            "clinic": None,
                            "clinic_type": None,
                            "case_type": None, 
                            "tags": None,
                            "country": 1,
                            "district": loc.id,
                            "region": locations[loc.parent_location].id,
                            "geolocation": from_shape(point).desc
                        }
                        found = True
                        break
            if not found:
                print("Not Found")
                return (None, None, None, None)
        except ValueError:
            print("Value Errot in point in polygon location")
            return (None, None, None, None)
    else:
        return (None, None, None, None)
    variables, variable_forms, variable_tests, variables_group, match_variables = variables
    variable_json = {}
    categories = {}
    for column in match_variables:
        row_value = row[location_form].get(column, None)
        if row_value not in ("", None):
            codes, cats = match_variables[column].get(row_value, [{}, {}])
            variable_json.update(codes)
            categories.update(cats)
    if "alert" in variable_json:
        for data_var in alert_data[location_form].keys():
            variable_json["alert_" + data_var] = row[
                location_form][alert_data[location_form][data_var]]
    disregard = False
    for group in variables.get(data_type, {}).keys():

        # Flag for whether the variable uses a priority system. A priority system allows variable values
        # with higher priority order to overwrite values with lower priority order.
        # Any variable in the group with priority data will set the flag to True
        priority_flag = False
        for v in variables[data_type][group]:
            if hasattr(variables[data_type][group][v],"calculation_priority") and \
               variables[data_type][group][v].calculation_priority not in ('', None):
                priority_flag = True
                intragroup_priority = 0  # Initialize the current priority level at zero
                current_group_variable = None
                break
            else:
                break
        # v is the primary key for the AggregationVariables table, not the string format id the data table refers the variables with
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

                #if there is no test outcome but there is another variable in the priority queue, test the variable next in prioritisation
                #if not test_outcome:
                #    test_outcome = variable_tests[v_backup](datum)

                if test_outcome:
                    if test_outcome == 1:
                        # This is done to allocate an integer into the
                        # test_outcome instead of a boolean value
                        test_outcome = 1

                    # fetch the string key for the current variable
                    variable_string_key = variables[data_type][group][v].variable.id

                    # Check whether the variable group uses a priority system
                    if priority_flag:
                        # This is the initial state
                        if intragroup_priority == 0:
                            variable_json[variables[data_type][
                                group][v].variable.id] = test_outcome  # insert new value
                            intragroup_priority = int(variables[data_type][
                                group][v].calculation_priority)  # store current intragroup priority
                            current_group_variable = variables[data_type][
                                group][v].variable.id  # store the variable id

                        # A higher priority order value is encountered
                        elif intragroup_priority > int(variables[data_type][group][v].calculation_priority): 
                            del variable_json[current_group_variable]  # remove existing group value of lower priority order
                            variable_json[variables[data_type][
                                group][v].variable.id] = test_outcome  # insert new value
                            intragroup_priority = int(variables[data_type][
                                group][v].calculation_priority)  # store current intragroup priority
                            current_group_variable = variables[data_type][
                                group][v].variable.id  # store the variable id

                        # Otherwise, do nothing
                    else:
                        #allocate the test outcome to the json object using the variable string id as key
                        variable_json[variables[data_type][
                            group][v].variable.id] = test_outcome

                    for cat in variables[data_type][
                            group][v].variable.category:
                        categories[cat] = variables[data_type][
                            group][v].variable.id
                    
                    if variables[data_type][group][v].variable.alert:
                        if variables[data_type][group][
                                v].variable.alert_type == "individual":
                            variable_json["alert"] = 1
                            variable_json["alert_type"] = "individual"
                            variable_json["alert_reason"] = variables[
                                data_type][group][v].variable.id
                            for data_var in alert_data[location_form].keys():
                                variable_json["alert_" + data_var] = row[
                                    location_form][alert_data[location_form][data_var]]
                    if variables[data_type][group][v].variable.disregard:
                        disregard = True

                    if not priority_flag: # When handling groups with priority order, loop through every variable
                        break  # We break out of the current group as all variables in a group are mutually exclusive

    if disregard and variable_json.get("alert_type", None) != "individual":
        disregard = False
    return (variable_json, categories, ret_location, disregard)
