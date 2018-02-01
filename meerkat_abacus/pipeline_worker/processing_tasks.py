from celery import Task, task
import logging


from meerkat_abacus.pipeline_worker.pipeline import Pipeline
from meerkat_abacus import util
from meerkat_abacus import config

config = config.get_config()


class PipelineTask(Task):
    """
    We only want to setup the pipeline once per worker

    """
    def __init__(self):
        engine, session = util.get_db_engine(config.DATABASE_URL)
        self.pipeline = Pipeline(engine, session, config)


@task(bind=True, base=PipelineTask)
def process_data(self, data_rows):
    self.pipeline.process_chunk(data_rows)
        
