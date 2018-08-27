import datetime

SUBMISSION_DATE_FORMAT = "%b %d, %Y %H:%M:%S %p"
PSQL_SUBMISSION_DATE_FORMAT = "Mon DD, YYYY HH:MI:SS AM"
PSQL_VISIT_DATE_FORMAT = "Mon DD, YYYY"

def strptime(date_string):
    return datetime.datetime.strptime(date_string, SUBMISSION_DATE_FORMAT)