"""
Main Celery App file

"""
from raven.contrib.celery import register_signal, register_logger_signal
import celery
import raven

from meerkat_abacus.pipeline_worker import celeryconfig
from meerkat_abacus import logger

# class Celery(celery.Celery):
#     def on_configure(self):
#         if config.sentry_dns:
#             client = raven.Client(config.sentry_dns)
#             # register a custom filter to filter out duplicate logs
#             register_logger_signal(client)
#             # hook into the Celery error handler
#             register_signal(client)


app = celery.Celery()
app.config_from_object(celeryconfig)
logger.info(celeryconfig.DEVELOPMENT)
if celeryconfig.DEVELOPMENT:
    # cleans up stale task between restarts
    pass
app.control.purge()

import meerkat_abacus.pipeline_worker.processing_tasks


if __name__ == "__main__":
    app.start()
