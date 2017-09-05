"""
Meerkat Abacus Test

Unit tests Meerkat Abacus
"""

from meerkat_abacus import config
from unittest import mock
import unittest
from meerkat_abacus import tasks


class CeleryTaskTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @mock.patch('meerkat_abacus.tasks.requests')
    @mock.patch('meerkat_libs.authenticate')
    def test_send_email_report(self, mock_authenticate, request_mock):
        mock_authenticate.return_value = 'meerkatjwt'
        report = 'test_report'
        config.mailing_root = ''
        # First check that no email is sent when mailing root is false.
        tasks.send_report_email(report, 'fr', "1")
        print(config.mailing_root)
        self.assertFalse(mock_authenticate.called)
        self.assertFalse(request_mock.request.called)
        # Then check that the email request is made when mailing root is set
        config.mailing_root = 'test_mailing_root'
        tasks.send_report_email(report, 'fr', "1")
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
