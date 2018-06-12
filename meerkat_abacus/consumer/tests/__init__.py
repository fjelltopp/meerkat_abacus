"""
Meerkat Abacus Test

Unit tests Meerkat Abacus
"""

from unittest import mock
import random
import unittest
from meerkat_abacus.consumer import get_data
from meerkat_abacus import config


class TestConsumer(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @mock.patch('meerkat_abacus.consumer.get_data.inspect')
    def test_read_data(self, inspect_mock):
        """
        Tests that read_stationary_data gets data from
        a get function and sends apropriate process_data.delay calls

        """
        inspect_mock_rv = mock.MagicMock()
        inspect_mock_rv.reserved = mock.MagicMock(return_value={"celery@abacus": []})
        inspect_mock.return_value = inspect_mock_rv
        param_config = config.get_config()
        param_config.country_config["tables"] = ["table1", "table2"]
        celery_app_mock = mock.MagicMock()
        get_data.read_stationary_data(yield_data_function,
                                      param_config, celery_app_mock, N_send_to_task=9)
                                      
        celery_app_mock.send_task.assert_called()
        self.assertEqual(celery_app_mock.send_task.call_count, 24)
        # 24 = 2 * 12. We get 11 normal calls and one extra for the last record
        
           
def yield_data_function(form, param_config=None, N=100):
    for i in range(N):
        yield {"a": random.random(),
               "b": random.random()}

        
if __name__ == "__main__":
    unittest.main()
