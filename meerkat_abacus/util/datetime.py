import datetime

FORM_TIMEFORMAT = "%b %d, %Y %H:%M:%S %p"

def strptime(date_string):
    return datetime.datetime.strptime(date_string, "%b %d, %Y %H:%M:%S %p")