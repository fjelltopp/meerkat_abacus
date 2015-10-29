BROKER_URL = 'amqp://guest@rabbit//'
CELERY_RESULT_BACKEND = 'rpc://guest@rabbit//'

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
#CELERY_TIMEZONE = 'Europe/Oslo'
CELERY_ENABLE_UTC = True
