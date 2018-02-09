from celery import Task, task
import logging


from meerkat_abacus.pipeline_worker.pipeline import Pipeline
from meerkat_abacus import util
from meerkat_abacus import config

config = config.get_config()
engine, session = util.get_db_engine(config.DATABASE_URL)


class PipelineTask(Task):
    """
    We only want to setup the pipeline once per worker

    """
    def __init__(self):
        logging.info("SETTING UP")

        self.pipeline = Pipeline(engine, session, config)
        self.session = session
        self.engine = engine
        
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        session.remove()


@task(bind=True, base=PipelineTask)
def process_data(self, data_rows):
    logging.info("STARTING task")
    self.pipeline.process_chunk(data_rows)
    logging.info("ENDING task")


@task
def test_up():
    return True
