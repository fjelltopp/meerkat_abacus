"""
Main functionality for importing data into abacus
"""
import logging
import boto3
import queue
from dateutil.parser import parse
import random
import yaml
from meerkat_abacus import model
from meerkat_abacus.config import config
from meerkat_abacus.codes import to_codes
from meerkat_abacus.util import data_types
from meerkat_abacus.util.epi_week import epi_week_for_date
from meerkat_libs import consul_client as consul

def read_stationary_data(get_function, internal_buffer,
                         buffer_proccesser_function, session,
                         engine, param_config=config):
    """
    Read stationary data using the get_function to determine the source
    """
    i = 0
    for form in param_config.country_config["tables"]:
        n = 0
        logging.info(form)
        uuid_field = "meta/instanceID"
        for element in get_function(form, param_config=param_config):
            try:
                i += 1
                n += 1
                uuid_field_current = param_config.country_config.get("tables_uuid",
                                                                     {}).get(form,
                                                                       uuid_field)
                internal_buffer.put_nowait({"form": form,
                                            "uuid": element[uuid_field_current],
                                            "data": element})
            except queue.Full:
                i = 0
                # Reached max_size of buffer
                buffer_proccesser_function(internal_buffer=internal_buffer,
                                           start=False,
                                           param_config_yaml=yaml.dump(param_config),
                                           run_overall_processes=False)
                internal_buffer.put({"form": form,
                                     "uuid": element[uuid_field_current],
                                     "data": element})
                logging.info("Processed {}".format(n))
            except KeyError:
                logging.warn("This element did not have a uuid_field {}".format(element))
        buffer_proccesser_function(internal_buffer=internal_buffer,
                                   start=False,
                                   param_config_yaml=yaml.dump(param_config))

def download_data_from_s3(config):
    """
    Get csv-files with data from s3 bucket

    Needs to be authenticated with AWS to run.

    Args:
       bucket: bucket_name
    """
    s3 = boto3.resource('s3')
    for form in config.country_config["tables"]:
        file_name = form + ".csv"
        s3.meta.client.download_file(config.s3_bucket, "data/" + file_name,
                                     config.data_directory + file_name)

def add_rows_to_db(form, form_data, session, engine,
                   uuid_field="meta/instanceID",
                   only_new=False,
                   deviceids=None,
                   table_name=None,
                   row_function=None,
                   quality_control=None,
                   allow_enketo=False,
                   start_dates=None,
                   exclusion_list=[],
                   fraction=None,
                   only_import_after_date=None,
                   param_config=config):
    """ Add form_data to DB
    If quality_control is true we look among the aggregation variables
    for variables of the import type. If this variable is not true the
    corresponding value is set to zero. If the variable has the disregard
    category we remove the whole row.

    Args:
        form: form_name
        form_data: list of data to be added
        session: SqlAlchemy session
        engine: SqlAlchemy engine
        only_new: If we should add only new data
        deviceids: if we should only add rows with a one of the deviceids
        table_name: name of table if different from filename
        row_function: function to appy to the rows before inserting
        start_dates: Clinic start dates, we do not add any data submitted
                     before these dates
        quality_control: If we are performing quality controll on the data.
        exclusion_list: A list of uuid's that are restricted from entering
        fraction: If present imports a randomly selected subset of data.
    """
    conn = engine.connect()

    table = model.form_tables(param_config=param_config)[form]
    exclusion_list = set(exclusion_list)

    if not only_new:
        uuids = set([row[uuid_field] for row in form_data])
        conn.execute(table.__table__.delete().where(
            table.__table__.c.uuid.in_(uuids)))
    else:
        uuids = set([row.uuid for row in session.query(table.uuid).all()])
        
    dicts = []

    new_rows = []
    to_check = []
    to_check_test = {}  # For speed
    logging.debug("Formname: %s", form)
    
    if quality_control:
        logging.debug("Doing Quality Control")
        (variables, variable_forms, variable_tests,
         variables_group, variables_match) = to_codes.get_variables(session, "import")
        if variables:
            to_check = [variables["import"][x][x]
                        for x in variables["import"].keys() if variables["import"][x][x].variable.form == form]
            for variable in to_check:
                to_check_test[variable] = variable.test
    removed = {}
    i = 0
    for row in form_data:
        if fraction:
            if random.random() > fraction:
                continue
        if only_import_after_date:
            if parse(row["SubmissionDate"]).replace(tzinfo=None) < only_import_after_date:
                continue
            
        if row[uuid_field] in exclusion_list:
            continue
        if only_new and row[uuid_field] in uuids:
            continue #  In this case we only add new data
        
        if "_index" in row:
            row["index"] = row.pop("_index")
        if row_function:
            insert_row = row_function(row)
        else:
            insert_row = row
        # If we have quality checks
        remove = False
        if to_check:
            for variable in to_check:
                try:
                    if not to_check_test[variable](insert_row):
                        if variable.variable.category == ["discard"]:
                            remove = True
                        else:
                            column = variable.column
                            if ";" in column or "," in column:
                                column = column.split(";")[0].split(",")[0]
                            category = variable.variable.category
                            replace_value = None
                            if category and len(category) > 0 and "replace:" in category[0]:
                                replace_column = category[0].split(":")[1]
                                replace_value = insert_row.get(replace_column,
                                                               None)
                            if column in insert_row:
                                insert_row[column] = replace_value
                                if insert_row[column]:
                                    if column in removed:
                                        removed[column] += 1
                                    else:
                                        removed[column] = 1
                except Exception as e:
                    logging.exception("Quality Controll error for code %s",variable.variable.id, exc_info=True)
        if remove:
            continue

        if deviceids:
            if should_row_be_added(insert_row, form, deviceids,
                                   start_dates, allow_enketo=allow_enketo):
                dicts.append({"data": insert_row,
                              "uuid": insert_row[uuid_field]})
                consul.send_dhis2_events(uuid=insert_row[uuid_field],
                                         form_id=form,
                                         raw_row=insert_row)
                new_rows.append(insert_row[uuid_field])
            else:
                logging.debug("Not added")
        else:
            dicts.append({"data": insert_row,
                          "uuid": insert_row[uuid_field]})
            consul.send_dhis2_events(uuid=insert_row[uuid_field],
                                     form_id=form,
                                     raw_row=insert_row)
            new_rows.append(insert_row[uuid_field])
        i += 1
        if i % 10000 == 0:
            conn.execute(table.__table__.insert(), dicts)
            dicts = []

    if to_check:
        logging.info("Quality Controll performed: ")
        logging.info("removed value: %s", removed)
    if len(dicts) > 0:
        conn.execute(table.__table__.insert(), dicts)
    conn.close()
    consul.flush_dhis2_events()
    logging.debug("Number of records %s", i)
    return new_rows


def should_row_be_added(row, form_name, deviceids, start_dates,
                        allow_enketo=False, param_config=config):
    """
    Determines if a data row should be added.
    If deviceid is not None, the reccord need to have one of the deviceids.
    If start_dates is not None, the record needs to be dated
    after the corresponding start date

    Args:
        row: row to be added
        form_name: name of form
        deviceids(list): the approved deviceid
        start_dates(dict): Clinic start dates
    Returns:
        should_add(Bool)
    """
    ret = False
    if deviceids is not None:
        if row.get("deviceid", None) in deviceids:
            ret = True
        else:
            if allow_enketo:
                for url in allow_enketo:
                    if url in row.get("deviceid", None):
                        ret = True
                        break
    else:
        ret = True
    if start_dates and row.get("deviceid", None) in start_dates:
        if not row["SubmissionDate"]:
            ret = False
        elif parse(row["SubmissionDate"]).replace(tzinfo=None) < start_dates[row["deviceid"]]:
            ret = False
    if ret:
        ret = _validate_date_to_epi_week_convertion(form_name, row, param_config=param_config)
    return ret


def _validate_date_to_epi_week_convertion(form_name, row, param_config=config):
    form_data_types = data_types.data_types_for_form_name(form_name,
                                                          param_config=param_config)
    if form_data_types:
        filters = []
        for form_data_type in form_data_types:
            filter = __create_filter(form_data_type)
            filters.append(filter)

        validated_dates = []
        for filter in filters:
            condition_field_name = filter.get('field_name')
            if not condition_field_name or __fulfills_condition(filter, row):
                if __should_discard_row(row, filter, validated_dates,
                                        param_config=param_config):
                    return False
    return True


def __create_filter(form_data_type):
    if form_data_type.get('condition'):
        return {
            'field_name': form_data_type['db_column'],
            'value': form_data_type['condition'],
            'date_field_name': form_data_type['date']
        }
    else:
        return {
            'date_field_name': form_data_type['date']
        }


def __fulfills_condition(filter, row):
    return row[filter['field_name']] == filter['value']


def __should_discard_row(row, filter, already_validated_dates, param_config=config):
    column_with_date_name = filter['date_field_name']
    if column_with_date_name in already_validated_dates:
        return False
    already_validated_dates.append(column_with_date_name)
    string_date = row[column_with_date_name]
    if not string_date:
        logging.debug(f"Empty value of date column for row with device_id: {row.get('deviceid')}" +
                        f" and submission date: {row.get('SubmissionDate')}")
        return True
    try:
        date_to_check = parse(string_date).replace(tzinfo=None)
        epi_week_for_date(date_to_check, param_config=param_config.country_config)
    except ValueError:
        logging.debug(f"Failed to process date column for row with device_id: {row.get('deviceid')}" +
                        f" and submission date: {row.get('SubmissionDate')}")
        return True
    return False
