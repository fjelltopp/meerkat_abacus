import csv

from meerkat_abacus.config import config


def data_types(param_config=config):
    with open(param_config.config_directory + param_config.country_config["types_file"],
              "r", encoding='utf-8',
              errors="replace") as f:

        DATA_TYPES_DICT = [_dict for _dict in csv.DictReader(f)]
    return DATA_TYPES_DICT


def data_types_for_form_name(form_name, param_config=config):
    return [data_type for data_type in data_types(param_config=param_config) if form_name == data_type['form']]
