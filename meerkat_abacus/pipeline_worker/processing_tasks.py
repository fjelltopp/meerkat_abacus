from celery.signals import worker_process_init
from celery import Task, task
import logging

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from meerkat_abacus.pipeline_worker.pipeline import Pipeline

from meerkat_abacus import util
from meerkat_abacus import config

config = config.get_config()


pipline = None


@worker_process_init.connect
def configure_workers(sender=None, conf=None, **kwargs):
    # load the application configuration
    # db_uri = conf['db_uri']
    global engine
    logging.info("Worker setup")
    engine = create_engine(config.DATABASE_URL,
                           pool_pre_ping=True)

    global session
    session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    logging.info(session)
    global pipeline
    pipeline = Pipeline(engine, session, config)


@task(bind=True)
def process_data(self, data_rows):
    logging.info("STARTING task")
    engine.dispose()
    pipeline.process_chunk(data_rows)
    logging.info("ENDING task")


@task
def test_up():
    return True
