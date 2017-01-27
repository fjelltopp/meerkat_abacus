"""
Functionality to create fake data
"""

import random
import datetime
import uuid


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
    elif field_type == "patient_id":
        value = random.randint(0, 10000)
    elif field_type == "range":
        upper, lower = argument
        value = random.uniform(upper, lower)
    elif field_type == "date":
        now = datetime.datetime.now()
        start = now - datetime.timedelta(days=21)
        total_days = (now - start).days
        date = start + datetime.timedelta(
            days=random.uniform(0, total_days))
        value = date.replace(hour=0,
                             second=0,
                             minute=0,
                             microsecond=0).isoformat()
    elif field_type == "data":
        if argument in data.keys():
            value = random.sample(data[argument], 1)[0]
        else:
            print("{} not in data".format(argument))
    else:
        value = None
    return value


def create_form(fields, data=None, N=500,odk=True):
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
            
            start = now - datetime.timedelta(days=21)
            total_days = (now - start).days
            start = start + datetime.timedelta(
                days=random.uniform(0, total_days))
            row["start"] = start.isoformat()
            end_total_days = (now - start).days
            end = start + datetime.timedelta(
                days=random.uniform(0, end_total_days))
            row["end"] = end.isoformat()
            submission_days = (now - end).days
            submission_date = end + datetime.timedelta(
                days=random.uniform(0, submission_days))
            row["SubmissionDate"] = submission_date.isoformat()
        list_of_records.append(row)

    return list_of_records
