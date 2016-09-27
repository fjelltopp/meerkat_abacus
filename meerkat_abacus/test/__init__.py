"""
Meerkat Abacus Test

Unit tests Meerkat Abacus
"""

import meerkat_abacus
from meerkat_abacus.test.db_test import *
from meerkat_abacus.test.link_test import *
from meerkat_abacus.test.to_codes_test import *
from meerkat_abacus.test.util_test import *
from meerkat_abacus.test.variable_test import *
from meerkat_abacus.task_queue import *
import unittest
from unittest import mock
#from meerkat_abacus.test.util_test import *

class CeleryTaskTest(unittest.TestCase):

    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    @mock.patch('meerkat_abacus.task_queue.requests')
    def test_send_email_report(self, request_mock):
        report = 'test_report'
        task_queue.send_report_email(report, 'fr', "1")
        self.assertTrue( request_mock.request.called )
        request_mock.request.assert_called_with( 
            'POST', 
            config.mailing_root + report +"/1/" ,
            json={"key": config.mailing_key}, 
            headers={'content-type': 'application/json'}     
        )
        

if __name__ == "__main__":
    unittest.main()
