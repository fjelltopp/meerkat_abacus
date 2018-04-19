import logging

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from meerkat_abacus.pipeline_worker.pipeline import Pipeline

from meerkat_abacus import config
from meerkat_abacus.pipeline_worker.celery_app import app

config_ = config.get_config()


pipeline = None


def configure_worker():
    # load the application configuration
    # db_uri = conf['db_uri']
    global engine
    logging.info("Worker setup")
    engine = create_engine(config_.DATABASE_URL, pool_pre_ping=True)

    global session
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=engine))
    logging.info(session)
    global pipeline
    pipeline = Pipeline(engine, session, config_)


@app.task(bind=True, name="processing_tasks.process_data")
def process_data(self, data_rows):
    if pipeline is None:
        configure_worker()
    logging.info("STARTING task")
    engine.dispose()
    pipeline.process_chunk(data_rows)
    logging.info("ENDING task")


@app.task(name="processing_tasks.test_up")
def test_up():
    return True
