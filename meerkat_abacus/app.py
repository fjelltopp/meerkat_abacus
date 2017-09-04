"""
Main Celery App file

"""
from raven.contrib.celery import register_signal, register_logger_signal
import celery
import raven
from meerkat_abacus import celeryconfig
from meerkat_abacus import config


class Celery(celery.Celery):
    def on_configure(self):
        if config.sentry_dns:
            client = raven.Client(config.sentry_dns)
            # register a custom filter to filter out duplicate logs
            register_logger_signal(client)
            # hook into the Celery error handler
            register_signal(client)


app = Celery()
app.config_from_object(celeryconfig)


if __name__ == "__main__":
    app.start()
