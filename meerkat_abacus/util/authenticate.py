import logging
import os

import backoff
import requests

from meerkat_libs import authenticate

ABACUS_AUTH_USERNAME = os.environ.get('ABACUS_AUTH_USERNAME', 'abacus-dev-user')
ABACUS_AUTH_PASSWORD = os.environ.get('ABACUS_AUTH_PASSWORD', 'password')
abacus_auth_token_ = ''


def retry_message(i):
    logging.info("Failed to authenticate. Retrying in " + str(i))

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      on_backoff=retry_message,
                      max_tries=8,
                      max_value=30)
@backoff.on_predicate(backoff.expo,
                      lambda x: x == '',
                      max_tries=10,
                      max_value=30)
def abacus_auth_token():
    global abacus_auth_token_
    abacus_auth_token_ = authenticate(username=ABACUS_AUTH_USERNAME,
                                      password=ABACUS_AUTH_PASSWORD,
                                      current_token=abacus_auth_token_)
    return abacus_auth_token_
