"""
Testing for DB utilities
"""

import unittest
from datetime import datetime, timedelta
from dateutil import parser
import io
from meerkat_abacus.util import create_fake_data, epi_week_start_date
from meerkat_abacus import util, model, config
from meerkat_abacus.config import country_config
from unittest import mock
from collections import namedtuple

class UtilTest(unittest.TestCase):

    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    def test_epi_week_start_date(self):
        first = datetime(2015, 1, 1)
        assert first == epi_week_start_date(2015, "international")

        first_saturday = datetime(2016,1,2)
        assert first_saturday == epi_week_start_date(2016, "day:5")

        first_tuesday = datetime(2016,1,5)
        assert first_tuesday == epi_week_start_date(2016, "day:1")

        first_wednesday = datetime(2016,1,6)
        assert first_wednesday == epi_week_start_date(2016, "day:2")

        test_config = {
            2016: datetime(2016, 1, 2),
            2017: datetime(2016, 12, 30)
        }
        assert datetime(2016, 1, 2)  == epi_week_start_date(2016, test_config)
        assert datetime(2016, 12, 30)  == epi_week_start_date(2017, test_config)
        
    def test_create_fake_data_get_value(self):
        """Test get value"""
        for i in range(100):
            value = create_fake_data.get_value(
                {"integer": [1, 2]}, None)
            self.assertIn(value, [1, 2])
            value = create_fake_data.get_value(
                {"one": ["one", "two"]}, None)
            self.assertIn(value, ["one", "two"])

        min_length = 3
        max_length = 0
        found_a = False
        found_b = False
        found_c = False
        for i in range(200):
            value = create_fake_data.get_value(
                {"multiple": ["A", "B", "C"]}, None)
            if "A" in value:
                found_a = True
            if "B" in value:
                found_b = True
            if "C" in value:
                found_c = True
            value = value.split(",")
            if len(value) > max_length:
                max_length = len(value)
            if len(value) < min_length:
                min_length = len(value)
        self.assertEqual(max_length, 3)
        self.assertEqual(min_length, 1)
        self.assertEqual(found_a, True)
        self.assertEqual(found_b, True)
        self.assertEqual(found_c, True)

        value = create_fake_data.get_value(
            {"date": "year"}, None)
        self.assertLess(parser.parse(value), datetime.now())
        self.assertLess(datetime.now() - timedelta(days=22),
                        parser.parse(value))
        data = {"deviceids": [1, 2, 3, 4, 5]}
        for i in range(100):
            value = create_fake_data.get_value(
                {"data": "deviceids"}, data)
            assert value in data["deviceids"]

    def test_create_fake_data(self):
        """
        Test creating form data
        """
        fields = {"a": {"integer": [1, 3]},
                  "b": {"one": ["one", "two"]}}
        records = create_fake_data.create_form(fields, N=499, odk=False)
        self.assertEqual(len(records), 499)

        tests = {
            1: False,
            2: False,
            3: False,
            "one": False,
            "two": False
        }
        for r in records:
            self.assertIn("a", r.keys())
            self.assertIn("b", r.keys())
            for t in tests:
                if t == r["a"] or t == r["b"]:
                    tests[t] = True
        for result in tests.values():
            self.assertEqual(result, True)
        data = {"deviceids": [1, 3]}
        
        records = create_fake_data.create_form(fields,
                                               N=50,
                                               data=data,
                                               odk=True)
        old_uuids = []
        for i, r in enumerate(records):
            self.assertIn("deviceid", r.keys())
            self.assertIn("start", r.keys())
            self.assertIn("end", r.keys())
            self.assertIn("SubmissionDate", r.keys())

            self.assertLess(parser.parse(r["start"]), datetime.now())
            self.assertLess(datetime.now() - timedelta(days=22),
                            parser.parse(r["start"]))
            self.assertLess(parser.parse(r["end"]), datetime.now())
            self.assertLess(datetime.now() - timedelta(days=22),
                            parser.parse(r["end"]))
            self.assertLess(parser.parse(r["SubmissionDate"]), datetime.now())
            self.assertLess(datetime.now() - timedelta(days=22),
                            parser.parse(r["SubmissionDate"]))
            self.assertIn("meta/instanceID", r.keys())
            self.assertNotIn(r["meta/instanceID"], old_uuids)
            old_uuids.append(r["meta/instanceID"])
            self.assertEqual(r["index"], i)

    def test_field_to_list(self):
        row = {"key": "a,b,c"}
        new_row = util.field_to_list(row, "key")
        self.assertEqual(new_row["key"], ["a", "b", "c"])
        row = {"key": "a,b;c"}
        new_row = util.field_to_list(row, "key")
        self.assertEqual(new_row["key"], ["a,b", "c"])
        row = {"key": "a"}
        new_row = util.field_to_list(row, "key")
        self.assertEqual(new_row["key"], ["a"])

    def test_read_csv(self):
        data = io.StringIO("A,B,C\na1,b1,c1\na2,b2,c2")
        # Fix to mock the interation in for row in reader
        with mock.patch('meerkat_abacus.util.open', return_value=data) as mo:
            rows = util.read_csv("test")
            rows = list(rows)
            mo.assert_called_with("test", 'r', encoding='utf-8', errors='replace')
            self.assertEqual(rows[0], {"A": "a1", "B": "b1", "C": "c1"})
            self.assertEqual(rows[1], {"A": "a2", "B": "b2", "C": "c2"})


    def test_write_csv(self):
        mo = mock.mock_open()
        with mock.patch('meerkat_abacus.util.open', mo):
            rows = [{"A": "a1", "B": "b1", "C": "c1"},
                    {"A": "a2", "B": "b2", "C": "c2"}]
            util.write_csv(rows, "test")
            mo.assert_called_with("test", 'w', encoding='utf-8')
            handle = mo()
            handle.write.assert_any_call('A,B,C\r\n')
            handle.write.assert_any_call('a1,b1,c1\r\n')
            handle.write.assert_any_call('a2,b2,c2\r\n')
            
    @mock.patch('meerkat_abacus.util.requests')
    def test_hermes(self, mock_requests):
        config.hermes_dev = True
        util.hermes("test", "POST", {"topics":["test-topic"]})
        self.assertFalse( mock_requests.request.called )
        config.hermes_dev = False
        util.hermes("test", "POST", {})
        headers = {'content-type': 'application/json'}
        mock_requests.request.assert_called_with( "POST",
                                                  config.hermes_api_root + "/test",
                                                  json={'api_key': config.hermes_api_key},
                                                  headers=headers )


    @mock.patch('meerkat_abacus.util.requests')
    def test_send_alert(self, mock_requests):
        alert = model.Data(**{"region": 2,
                              "clinic": 3,
                              "district": 4,
                              "uuid": "uuid:1",
                              "variables": {"alert_reason": "1",
                                            "alert_id": "abcdef",
                                            "alert_gender": "male",
                                            "alert_age": "32"},
                              "id": "abcdef",
                              "date": datetime.now()
        })
        var_mock = mock.Mock()
        var_mock.configure_mock(name='Rabies')

        region_mock = mock.Mock()
        region_mock.configure_mock(name="Region")

        clinic_mock = mock.Mock()
        clinic_mock.configure_mock(name="Clinic")

        district_mock = mock.Mock()
        district_mock.configure_mock(name="District")
        
        variables = {"1": var_mock}
        locations = {
            2: region_mock,
            3: clinic_mock,
            4: district_mock
        }
                                        
        util.country_config["messaging_silent"] = False
        util.send_alert("abcdef", alert, variables, locations)
        self.assertTrue(mock_requests.request.called)
        call_args = mock_requests.request.call_args
        self.assertEqual(call_args[0][0], "PUT")
        self.assertEqual(call_args[0][1],
                         config.hermes_api_root + "/publish")
        self.assertTrue( len(call_args[1]["json"]["sms-message"]) < 160 ) #160 characters in a single sms
        self.assertIn("Rabies", call_args[1]["json"]["html-message"])
        self.assertIn("Rabies", call_args[1]["json"]["sms-message"])
        self.assertIn("Rabies", call_args[1]["json"]["message"])
       
        prefix = util.country_config["messaging_topic_prefix"]
        self.assertIn(prefix + "-1-allDis", call_args[1]["json"]["topics"])
        self.assertIn(prefix + "-2-allDis", call_args[1]["json"]["topics"])
        self.assertIn(prefix + "-1-1", call_args[1]["json"]["topics"])
        self.assertIn(prefix + "-2-1", call_args[1]["json"]["topics"])
        self.assertEqual("abcdef", call_args[1]["json"]["id"])
    
        # The date is now too early
        mock_requests.reset_mock()
        alert.date = datetime.now() - timedelta(days=8)
        util.send_alert("abcdef", alert, variables, locations)
        self.assertFalse(mock_requests.request.called)

    def test_create_topic_list(self):
        #Create the mock arguments that include all necessary data to complete the function.
        AlertStruct = namedtuple("AlertStruct", 'variables clinic region')
        LocationStruct = namedtuple( "LocationStruct", "parent_location" )
        alert = AlertStruct( variables={"alert_reason":"rea_1"}, clinic="4", region="2" )
        locations = { "4": LocationStruct( parent_location="3" ) }

        #Call the method
        rv = util.create_topic_list(alert, locations)
        
        #Check the return value is as expected.
        def pref(string):
            return country_config["messaging_topic_prefix"] + "-" + string
        expected = [ 
            pref("4-rea_1"),
            pref("3-rea_1"),
            pref("2-rea_1"),
            pref("1-rea_1"),
            pref("4-allDis"),
            pref("3-allDis"),
            pref("2-allDis"),
            pref("1-allDis")
        ]
        self.assertEqual( set(rv), set(expected) )
            
        #If the parent location of the clinic is a region, check that no district is included.
        locations = { "4": LocationStruct( parent_location="2" ) }
        rv = util.create_topic_list(alert, locations)
        expected = [ 
            pref("4-rea_1"),
            pref("2-rea_1"),
            pref("1-rea_1"),
            pref("4-allDis"),
            pref("2-allDis"),
            pref("1-allDis")
        ]
        self.assertEqual( set(rv), set(expected) )
            
        

        
if __name__ == "__main__":
    unittest.main()
