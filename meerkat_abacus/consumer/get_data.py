import logging
import boto3
import time
import json
from celery.task.control import inspect

from meerkat_abacus.util import create_fake_data
from meerkat_abacus import util


def read_stationary_data(get_function, param_config, celery_app, N_send_to_task=5000,
                         previous_number_by_form={}):
    """
    Read stationary data using the get_function to determine the source
    """
    celery_inspect = inspect()

    number_by_form = {}
    for form_name in param_config.country_config["tables"]:

        start = previous_number_by_form.get(form_name, 0)
        logging.info(f"Start processing data for form {form_name}")
        data = []
        for i, element in enumerate(get_function(form_name, param_config=param_config)):
            if i < start:
                next
            data.append({"form": form_name,
                         "data": dict(element)})
            if i % N_send_to_task == 0:
                logging.info(f"Processed {i} records")
                send_task(data, celery_app, celery_inspect)
                data = []
        if data:
            send_task(data, celery_app, celery_inspect)
        logging.info("Finished processing data.")
        logging.info(f"Processed {i} records")
        number_by_form[form_name] = i
    return number_by_form


def get_N_tasks(inspect, name):
    registered = len(inspect.registered()[name])
    reserved = len(inspect.reserved()[name])
    logging.info(f"registered {registered}, reserved {reserved}")
    return reserved + registered


def send_task(data, celery_app, inspect, N=15):
    """
    Sends data to process queue if the the are less than N tasks waiting


    """
    while get_N_tasks(inspect, "celery@abacus") > N:
        logging.info("There were too many reserved tasks so waiting 5 seconds")
        time.sleep(5)

    logging.info("Sending data")
    celery_app.send_task("processing_tasks.process_data", [data])

        
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

        
# Real time

def real_time_s3(app, config, session, number_by_form):
    """ Downloads data from S3 and adds new data from the CSV files"""
    logging.info("Starting read from S3")
    download_data_from_s3(config)
    read_stationary_data(util.read_csv_file, config, previous_number_by_form=number_by_form)
    logging.info("Finishing read from S3")
    time.sleep(config.data_stream_interval)
    return number_by_form
    

def real_time_fake(app, config, session):
    """ Creates new fake data and adds it to the system"""
    logging.info("Sending fake data")
    new_data = []
    for form in config.country_config["tables"]:
        data = create_fake_data.get_new_fake_data(form=form,
                                                  session=session,
                                                  N=10,
                                                  param_config=config,
                                                  dates_is_now=True)
        new_data += [{"form": form, "data": d[0]} for d in data]
    if config.fake_data_generation == "INTERNAL":
        app.send_task('processing_tasks.process_data', [new_data])
    elif config.fake_data_generation == "SEND_TO_SQS":
        sqs_client, sqs_queue_url = util.subscribe_to_sqs(config.fake_data_sqs_endpoint,
                                                          config.fake_data_sqs_queue.lower())
        for d in new_data:
            d["formId"] = d["form"]
            sqs_client.send_message(
                QueueUrl=sqs_queue_url,
                MessageBody=json.dumps(d))

        for i in range(4):
            real_time_sqs(app, config)
    else:
        raise NotImplementedError("Not yet implemented")
    logging.info("Sleeping")
    time.sleep(config.fake_data_interval)


sqs_client = None
sqs_queue_url = None


def real_time_sqs(app, config, *args):
    """ Reads data from AWS SQS"""
    global sqs_client
    global sqs_queue_url
    if sqs_client is None:
        try:
            sqs_client, sqs_queue_url = util.subscribe_to_sqs(config.SQS_ENDPOINT,
                                                              config.sqs_queue.lower())
        except Exception as e:
            logging.exception("Error in reading message", exc_info=True)
            return
    try:
        logging.info("Getting messages from queue " + str(sqs_queue_url))
        messages = sqs_client.receive_message(QueueUrl=sqs_queue_url,
                                              WaitTimeSeconds=19,
                                              MaxNumberOfMessages=10)
    except Exception as e:
        logging.error(str(e) + ", retrying...")
        return
    if "Messages" in messages:
        messages_to_send = []
        for message in messages["Messages"]:
            logging.info("Message %s", message)
            receipt_handle = message["ReceiptHandle"]
            logging.debug("Deleting message %s", receipt_handle)

            try:
                message_body = json.loads(message["Body"])
                form = message_body["formId"]
                form_data = message_body["data"]
                messages_to_send.append({"form": form, "data": form_data})
                sqs_client.delete_message(QueueUrl=sqs_queue_url,
                                          ReceiptHandle=receipt_handle)
            except Exception as e:
                logging.exception("Error in reading message", exc_info=True)
        app.send_task("processing_tasks.process_data", [messages_to_send])
