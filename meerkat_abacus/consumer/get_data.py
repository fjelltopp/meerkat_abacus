import logging
import boto3

from meerkat_abacus.pipeline_worker.processing_tasks import process_data


def read_stationary_data(get_function, param_config, N_send_to_task=100):
    """
    Read stationary data using the get_function to determine the source
    """
    for form in param_config.country_config["tables"]:
        logging.info(form)
        data = []
        for element in get_function(form, param_config=param_config):
            data.append({"form": form,
                         "data": dict(element)})
            if len(data) == N_send_to_task:
                process_data.delay(data)
                data = []
        if data:
            process_data.delay(data)


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

        

