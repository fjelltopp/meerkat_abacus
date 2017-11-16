import csv

from meerkat_abacus import config


def init_data_types():
    with open(config.config_directory + config.country_config["types_file"], "r", encoding='utf-8',
              errors="replace") as f:
        return [_dict for _dict in csv.DictReader(f)]


DATA_TYPES_DICT = init_data_types()


def data_types_for_form_name(form_name):
    return [data_type for data_type in DATA_TYPES_DICT if form_name == data_type['form']]
