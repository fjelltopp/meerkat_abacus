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
                uuid_field_current = param_config.country_config.get(
                    "tables_uuid",
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

        

