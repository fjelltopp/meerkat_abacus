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

    @mock.patch('meerkat_abacus.consumer.get_data.process_data.delay')
    def test_read_data(self, process_data_mock):
        """
        Tests that read_stationary_data gets data from
        a get function and sends apropriate process_data.delay calls

        """
        param_config = config.get_config()
        param_config.country_config["tables"] = ["table1", "table2"]
        
        get_data.read_stationary_data(yield_data_function,
                                      param_config, N_send_to_task=9)
                                      
        process_data_mock.assert_called()
        self.assertEqual(process_data_mock.call_count, 24)
        # 24 = 2 * 12. We get 11 normal calls and one extra for the last record
        

def yield_data_function(form, param_config=None, N=100):
    for i in range(N):
        yield {"a": random.random(),
               "b": random.random()}

        
if __name__ == "__main__":
    unittest.main()
