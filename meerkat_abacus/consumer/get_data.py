import logging
import boto3
import time
from celery.task.control import inspect

from meerkat_abacus.pipeline_worker import processing_tasks


def read_stationary_data(get_function, param_config, N_send_to_task=1000):
    """
    Read stationary data using the get_function to determine the source
    """
    celery_inspect = inspect()

    for form_name in param_config.country_config["tables"]:
        logging.info(f"Start processing data for form {form_name}")
        data = []
        for i, element in enumerate(get_function(form_name, param_config=param_config)):
            data.append({"form": form_name,
                         "data": dict(element)})
            if i % N_send_to_task == 0:
                logging.info(f"Processed {i} records")
                send_task(data, celery_inspect)
                data = []
        if data:
            send_task(data, celery_inspect)
            logging.info("Cleaning up data buffer.")
        logging.info("Finished processing data.")

def send_task(data, inspect, N=20):
    """
    Sends data to process queue if the the are less than N tasks waiting


    """

    inspect_result = inspect.reserved()["celery@abacus"]
    logging.info(inspect_result)
    while len(inspect_result) > N:
        logging.info("There where {} reserved tasks so waiting 5 seconds".format(
            len(inspect_result)))
        time.sleep(5)
        inspect_result = inspect.reserved()["celery@abacus"]
    logging.info("Sending data")
    processing_tasks.process_data.delay(data)

        
def download_data_from_s3(config):
    """
    Get csv-files with data from s3 bucket

    Needs to be authenticated with AWS to run.

    Args:
       bucket: bucket_name
    """
    s3 = boto3.resource('s3')
    for form_name in config.country_config["tables"]:
        file_name = form_name + ".csv"
        s3_key = "data/" + file_name
        destination_path = config.data_directory + file_name
        s3.meta.client.download_file(config.s3_bucket, s3_key, destination_path)

        

