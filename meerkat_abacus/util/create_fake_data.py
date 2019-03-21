"""
Functionality to create fake data
"""

import random
import datetime
import uuid
from meerkat_abacus import util
from meerkat_abacus import model
from meerkat_abacus.config import config

logger = config.logger

random.seed(1)

def get_value(field, data):
    """
    Takes a field and returns the value
    
    A field is a dict with a key that gives the method to choose from a value for the dict value.
    I.e.
      field = {"one": ["A", "B", "C"]}
    We then want to choose either A, B, or C randomly.

    The available methos are:
    one: choose one of the items in the list
    integer: choose an intenger between [uppper, lower]
    multiple: choose a random subset of the list
    date: choose a date in the last three weeks
    data: the value gives a key that should exist in the data dict. We choose one value from the list in the data dict


    Args:
        field: a field
        data: data to be used for certain field types
    Returns:
        value: A random value for the field
    """
    field_type = list(field)[0]
    argument = field[field_type]
    if field_type == "integer":
        upper, lower = argument
        value = random.randint(upper, lower)
    elif field_type == "one":
        value = random.sample(argument, 1)[0]
    elif field_type == "multiple":
        number_of_options = random.randint(1, len(argument))
        value = ",".join(random.sample(argument, number_of_options))
    elif field_type == "multiple-spaces":
        number_of_options = random.randint(1, len(argument))
        value = " ".join(random.sample(argument, number_of_options))
    elif field_type == "patient_id":
        value = random.randint(0, 10000)
    elif field_type == "range":
        upper, lower = argument
        value = random.uniform(upper, lower)
    elif field_type == "date":
        now = datetime.datetime.now()
        start_offset = 21
        if argument == "age":
            start_offset = 365*80
        start = now - datetime.timedelta(days=start_offset)
        total_days = (now - start).days
        date = start + datetime.timedelta(
            days=random.uniform(0, total_days))
        value = date.replace(hour=0,
                             second=0,
                             minute=0,
                             microsecond=0).isoformat()
    elif field_type == "data":
        if argument in data.keys():
            if len(data[argument]) == 0:
                value = None
            else:
                value = random.sample(data[argument], 1)[0]
        else:
            print("{} not in data".format(argument))
    else:
        value = None
    return value


def create_form(fields, data=None, N=500, odk=True, dates_is_now=False):
    """
    Creates a csv file with data form the given fields

    The types for fields are:

    {"integer": [lower, upper]}
        random int between upper and lower
    {"one": ["choice1",choice2",....]}
       one random choice from the list
    {"multiple: ["choice1",choice2",....]}
       a random subset of choices
    {"data: "key"}
       a random choice from key in data

    Args:
        from_name: name of the form
        fields: list of fields to include
        previous_data: data from other forms
        N: number of rows to generate
        odk: Does the form come from odk

    Returns:
        list_of_records(list): list of dicts with data

    """
    logger.debug("Creating fields: " + str(fields))
    logger.debug("number of records: " + str(N))
    list_of_records = []
    for i in range(N):
        row = {}
        unique_ids = {}
        for field_name in fields.keys():
            if field_name != "deviceids": # We deal with deviceid in the odk part below
                value = get_value(fields[field_name], data)
                row[field_name] = value
        for field_name in fields.keys():
            if field_name != "deviceids" and list(fields[field_name].keys())[0] == "patient_id":
                unique_ids.setdefault(field_name, list())
                unique_field, unique_condition = fields[field_name]["patient_id"].split(";")
                if row[unique_field] == unique_condition:
                    current_id = row[field_name]
                    while current_id in unique_ids[field_name]:
                        current_id = random.randint(0, 100000)
                    row[field_name] = current_id
                    unique_ids[field_name].append(row[field_name])
                else:
                    if field_name in unique_ids and len(unique_ids[field_name]) > 1:
                        row[field_name] = random.sample(unique_ids[field_name], 1)[0]
                    else:
                        row[field_name] = random.randint(0, 10000)
                        
        if odk:
            # If we are creating fake data for an odk form, we want to add a number of special fields
            if "deviceids" in data.keys():
                row["deviceid"] = random.sample(data["deviceids"],
                                                1)[0]
            else:
                print("No deviceids given for an odk form")
            row["index"] = i
            row["meta/instanceID"] = "uuid:" + str(uuid.uuid4())
            now = datetime.datetime.now()

            if dates_is_now:
                start = now - datetime.timedelta(minutes=1)
                end = now
                submission_date = now
            else:
                start = now - datetime.timedelta(days=21)
                total_days = (now - start).days
                start = start + datetime.timedelta(
                    days=random.uniform(0, total_days))

                end_total_days = (now - start).days
                end = start + datetime.timedelta(
                    days=random.uniform(0, end_total_days))

                submission_days = (now - end).days
                submission_date = end + datetime.timedelta(
                    days=random.uniform(0, submission_days))

            
            
            row["end"] = end.isoformat()
            row["start"] = start.isoformat()
            row["SubmissionDate"] = submission_date.isoformat()
        list_of_records.append(row)

    return list_of_records


def get_new_fake_data(form, session, N, param_config=None, dates_is_now=False):
    logger.debug("fake data")
    deviceids = util.get_deviceids(session, case_report=True)

    # Make sure the case report form is handled before the alert form
    logger.debug("Processing form: %s", form)
    if form not in param_config.country_config["fake_data"]:
        return []
    if "deviceids" in param_config.country_config["fake_data"][form]:
        # This is a special way to limit the deviceids for a form in
        # the config file
        form_deviceids = param_config.country_config["fake_data"][form]["deviceids"]
    else:
        form_deviceids = deviceids
    alert_ids = []
    for value in param_config.country_config["fake_data"][form]:
        if "data" in value and value["data"] == "uuids" and "from_form" in value:
            from_form = value["from_form"]
            table = model.form_tables(param_config=param_config)[from_form]
            uuids = [r[0] for r in session.query(table.uuid).all()]
            for row in uuids:
                alert_ids.append(row[-param_config.country_config["alert_id_length"]:])

    data = create_form(
        fields=param_config.country_config["fake_data"][form],
        data={"deviceids":
              form_deviceids,
              "uuids": alert_ids},
        N=N,
        dates_is_now=dates_is_now)
    return [(row, row["meta/instanceID"]) for row in data]


def create_fake_data(session, config, N=500, append=False,
                     from_files=False,
                     write_to="file"):
    """
    Creates a csv file with fake data for each form. We make
    sure that the forms have deviceids that match the imported locations.

    For the case report forms we save the X last characters of
    meta/instanceID to use as alert_ids for the alert_form,
    where X is the alert_id_lenght from the config file.

    Args:
       session: SQLAlchemy session
       N: number of rows to create for each from (default=500)
       append: If we should append the new fake data or write
               over the old (default=False)
       from_files: whether to add data from the manual test case
                   files defined in country_config
    """
    logger.debug("fake data")
    deviceids = util.get_deviceids(session, case_report=True)
    alert_ids = []
    country_config = config.country_config
    forms = country_config["tables"]
    # Make sure the case report form is handled before the alert form
    for form in forms:
        logger.debug("Processing form: %s", form)
        file_name = config.data_directory + form + ".csv"
        current_form = []
        if form not in country_config["fake_data"]:
            continue
        if append:
            current_form = util.read_csv(file_name)
        if "deviceids" in country_config["fake_data"][form]:
            # This is a special way to limit the deviceids for a form in
            # the config file
            form_deviceids = country_config["fake_data"][form]["deviceids"]
        else:
            form_deviceids = deviceids

        manual_test_data = {}
        if from_files and form in country_config.get("manual_test_data", {}).keys():
            current_directory = os.path.dirname(os.path.realpath(__file__))
            for fake_data_file in country_config.get("manual_test_data", {})[form]:
                manual_test_data[fake_data_file] = []
                logger.debug("Adding test data from file: %s.csv", fake_data_file)
                manual_test_data[fake_data_file] = util.read_csv(current_directory + '/test/test_data/test_cases/' + \
                                                                 fake_data_file + ".csv")

        generated_data = create_form(
            country_config["fake_data"][form], data={"deviceids":
                                                     form_deviceids,
                                                     "uuids": alert_ids}, N=N)

        if "case" in form:
            alert_ids = []
            for row in generated_data:
                alert_ids.append(row["meta/instanceID"][-country_config[
                    "alert_id_length"]:])
        manual_test_data_list = []
        for manual_test_data_file in manual_test_data.keys():
            manual_test_data_list += list(manual_test_data[manual_test_data_file])
        for row in manual_test_data_list:
            if len(generated_data) > 0:
                for key in generated_data[0].keys():
                    if key not in row:
                        row[key] = None
        data_to_write = list(current_form) + list(manual_test_data_list) + generated_data
        if write_to == "file":
            util.write_csv(data_to_write, file_name)
        elif write_to == "local_db":
            util.write_to_db(data_to_write, form,
                             param_config.PERSISTENT_DATABASE_URL,
                             param_config=param_config)



