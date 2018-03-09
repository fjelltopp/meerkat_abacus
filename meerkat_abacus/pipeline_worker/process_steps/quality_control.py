"""
Main functionality for importing data into abacus
"""

import logging
from dateutil.parser import parse
import random
from meerkat_abacus import util
from meerkat_abacus.util import data_types
from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus.util.epi_week import epi_week_for_date
from meerkat_abacus.codes import to_codes


class QualityControl(ProcessingStep):
    def __init__(self, param_config, session):
        """ Prepare arguments for quality_control

            deviceids: if we should only add rows with a one of the deviceids
            row_function: function to appy to the rows before inserting
            start_dates: Clinic start dates, we do not add any data submitted
                     before these dates
            quality_control: If we are performing quality controll on the data.
            exclusion_list: A list of uuid's that are restricted from entering
            fraction: If present imports a randomly selected subset of data.
        """
        self.step_name = "quality_control"
        self.session = session
        config = {}
        for form in param_config.country_config["tables"]:
            deviceids_case = util.get_deviceids(session, case_report=True)
            deviceids = util.get_deviceids(session)
            
            start_dates = util.get_start_date_by_deviceid(session)
            exclusion_list = set(util.get_exclusion_list(session, form))
            uuid_field = "meta/instanceID"
            if "tables_uuid" in param_config.country_config:
                uuid_field = param_config.country_config["tables_uuid"].get(form, uuid_field)
            if form in param_config.country_config["require_case_report"]:
                form_deviceids = deviceids_case
            else:
                form_deviceids = deviceids
            if "no_deviceid" in param_config.country_config and form in param_config.country_config["no_deviceid"]:
                form_deviceids = []
            quality_control = {}
            quality_control_list = []
            if "quality_control" in param_config.country_config:
                if form in param_config.country_config["quality_control"]:
                    (variables, variable_forms, variable_tests,
                     variables_group, variables_match) = to_codes.get_variables(session,
                                                                                "import")
                    if variables:
                        quality_control_list = [variables["import"][x][x]
                                    for x in variables["import"].keys() if variables["import"][x][x].variable.form == form]
                    for variable in quality_control_list:
                        quality_control[variable] = variable.test

            quality_control = quality_control
            allow_enketo = False
            if form in param_config.country_config.get("allow_enketo", []):
                allow_enketo = param_config.country_config["allow_enketo"][form]

            config[form] = {"uuid_field": uuid_field,
                            "deviceids": form_deviceids,
                            "table_name": form,
                            "only_new": True,
                            "start_dates": start_dates,
                            "quality_control": quality_control,
                            "allow_enketo": allow_enketo,
                            "exclusion_list": exclusion_list,
                            "fraction": param_config.import_fraction,
                            "only_import_after_date": param_config.only_import_after_date,
                            "param_config": param_config}
        self.config = config
        self.param_config = param_config

    def run(self, form, row):
        """
        Does quality control to change any needed data
        and to check that data should be added

        Args:
        form: form_name
        row: data_row
        """
        config = self.config[form]
        if self._exclude_by_start_date_or_fraction(row, form):
            return []
        
        if row[config["uuid_field"]] in config["exclusion_list"]:
            return []
        
        # If we have quality checks
        remove = self._do_quality_control(row, form)
        if remove:
            return []
        if config["deviceids"]:
            if not should_row_be_added(row, form, config["deviceids"],
                                       config["start_dates"],
                                       self.param_config,
                                       allow_enketo=config["allow_enketo"]):
                return []
        flatten_structure(row)
        return [{"form": form,
                 "data": row}]
    
    def _exclude_by_start_date_or_fraction(self, row, form):
        if self.config[form]["fraction"]:
            if random.random() > self.config[form]["fraction"]:
                return True
        if self.config[form]["only_import_after_date"]:
            submission_date = parse(row["SubmissionDate"]).replace(tzinfo=None)
            if submission_date < self.config[form]["only_import_after_date"]:
                return True
        return False

    def _do_quality_control(self, insert_row, form):
        remove = False
        quality_control = self.config[form]["quality_control"]
        if quality_control:
            for variable in quality_control:
                try:
                    if not quality_control[variable](insert_row):
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
                except Exception as e:
                    logging.exception("Quality Controll error for code %s",variable.variable.id, exc_info=True)
        return remove

    
def flatten_structure(row):
    """
    Flattens all lists in row to comma separated strings"
    """
    for key, value in row.items():
        if isinstance(value, list):
            row[key] = ",".join(value)


def should_row_be_added(row, form_name, deviceids, start_dates, param_config,
                        allow_enketo=False):
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
        elif parse(
                row["SubmissionDate"]).replace(tzinfo=None) < start_dates[row["deviceid"]]:
            ret = False
    if ret:
        ret = _validate_date_to_epi_week_convertion(form_name, row, param_config)
    return ret


def _validate_date_to_epi_week_convertion(form_name, row, param_config):
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


def __should_discard_row(row, filter, already_validated_dates, param_config):
    column_with_date_name = filter['date_field_name']
    if "$" in column_with_date_name:
        column_with_date_name.replace("$", "1")
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



