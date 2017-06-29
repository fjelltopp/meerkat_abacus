"""
Meerkat Abacus Test

Unit tests Meerkat Abacus
"""

import meerkat_abacus
from meerkat_abacus import config, task_queue
from meerkat_abacus.test.db_test import *
from meerkat_abacus.test.link_test import *
from meerkat_abacus.test.to_codes_test import *
from meerkat_abacus.test.util_test import *
from meerkat_abacus.test.variable_test import *
from unittest import mock
import unittest


class CeleryTaskTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @mock.patch('meerkat_abacus.task_queue.requests')
    @mock.patch('meerkat_libs.authenticate')
    def test_send_email_report(self, mock_authenticate, request_mock):
        mock_authenticate.return_value = 'meerkatjwt'
        report = 'test_report'
        # First check that no email is sent when mailing root is false.
        task_queue.send_report_email(report, 'fr', "1")
        self.assertFalse(mock_authenticate.called)
        self.assertFalse(request_mock.request.called)
        # Then check that the email request is made when mailing root is set
        config.mailing_root = 'test_mailing_root'
        task_queue.send_report_email(report, 'fr', "1")
        self.assertTrue(mock_authenticate.called)
        self.assertTrue(request_mock.request.called)
        request_mock.request.assert_any_call(
            'POST',
            config.auth_root + '/api/login',
            json={"username": "report-emails",
                  "password": config.mailing_key},
            headers={'content-type': 'application/json'}
        )


if __name__ == "__main__":
    unittest.main()
