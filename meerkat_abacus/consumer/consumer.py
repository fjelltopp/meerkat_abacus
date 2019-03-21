import logging
import celery
from celery import Celery
from celery.task.control import inspect
import time
import backoff

from meerkat_abacus.consumer import celeryconfig
from meerkat_abacus.consumer import database_setup
from meerkat_abacus.consumer import get_data
from meerkat_abacus.config import get_config
from meerkat_abacus import util, model
from meerkat_abacus.util import create_fake_data


config = get_config()

logging.getLogger().setLevel(logging.INFO)

app = Celery()
app.config_from_object(celeryconfig)
app.conf.task_default_queue = 'abacus'
start_time = time.time()
session, engine = database_setup.set_up_database(False, True, config)


@backoff.on_exception(backoff.expo,
                      (celery.exceptions.TimeoutError,
                       AttributeError, OSError),
                      max_tries=10,
                      max_value=30)
def wait_for_celery_runner():
    test_task = app.send_task('processing_tasks.test_up')
    result = test_task.get(timeout=1)
    return result


wait_for_celery_runner()
# Initial Setup


database_setup.unlogg_tables(config.country_config["tables"], engine)

logging.info("Starting initial setup")

if config.initial_data_source == "AWS_S3":
    get_data.download_data_from_s3(config)
    get_function = util.read_csv_file
elif config.initial_data_source == "LOCAL_CSV":
    get_function = util.read_csv_file
elif config.initial_data_source == "FAKE_DATA":
    get_function = util.read_csv_file
    create_fake_data.create_fake_data(session,
                                      config,
                                      write_to="file")

elif config.initial_data_source in ["AWS_RDS", "LOCAL_RDS"]:
    get_function = util.get_data_from_rds_persistent_storage
else:
    raise AttributeError(f"Invalid source {config.initial_data_source}")

number_by_form = get_data.read_stationary_data(get_function, config, app)


database_setup.logg_tables(config.country_config["tables"], engine)

# Wait for initial setup to finish

celery_inspect = inspect()
for i in range(15):
    celery_queues = celery_inspect.reserved()
    inspect_result = celery_queues.get("celery@abacus", [])
    if len(inspect_result) > 0:
        break
    logging.info(f"Avaiable celery queues: {inspect_result}")
    time.sleep(20)
else:
    setup_time = round(time.time() - start_time)
    logging.error(f"Failed to wait for message queue after {setup_time} seconds.")
setup_time = round(time.time() - start_time)
logging.info(f"Finished setup in {setup_time} seconds")

failures = session.query(model.StepFailiure).all()

if failures:
    N_failures = len(failures)
    logging.error(f"There were{N_failures} records that failed in the pipeline, see the step_failures database table for more information")
    
   
run_dict = {
    "AWS_S3": get_data.real_time_s3,
    "FAKE_DATA": get_data.real_time_fake,
    "AWS_SQS": get_data.real_time_sqs
}
sds = config.stream_data_source
while True:
    try:
        number_by_form = run_dict[sds](app, config, session, number_by_form)
    except KeyError:
        RuntimeError("Unsupported data source.")
    except:
        logging.exception("Error in real time", exc_info=True)



