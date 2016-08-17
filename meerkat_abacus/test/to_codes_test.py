import unittest
import datetime

from meerkat_abacus import model
from meerkat_abacus.codes.to_codes import to_code
from meerkat_abacus.codes.variable import Variable

# Data for the tests
locations = {1: model.Locations(name="Demo"),
             2: model.Locations(name="Region 1",
                                parent_location=1),
             3: model.Locations(name="Region 2",
                                parent_location=1),
             4: model.Locations(name="District 1",
                                parent_location=2),
             5: model.Locations(name="District 2",
                                parent_location=3),
             6: model.Locations(name="Clinic 1",
                                parent_location=4),
             7: model.Locations(name="Clinic 2",
                                parent_location=5),
             8: model.Locations(name="Clinic with no district",
                                parent_location=2)
}
# locations_by_deviceid = {1: 6, 2: 7, 3: 8}
# regions = [2, 3]
# districts = [4, 5]
# agg_variables = [
#     model.AggregationVariables(
#         id=1,
#         method="count",
#         db_column="index",
#         form="form1"),
#     model.AggregationVariables(
#         id=2,
#         method="count_occurrence",
#         db_column="column1",
#         alert=1,
#         condition="A",
#         form="form1"),
#     model.AggregationVariables(
#         id=3,
#         method="count_occurrence_in",
#         db_column="column2",
#         condition="B",
#         form="form1"),
#     model.AggregationVariables(
#         id=4,
#         method="int_between",
#         db_column="column3",
#         condition="5,10",
#         form="form1")
# ]
# alert_data = {"column1": "column1"}
# all_locations = (locations,
#                  locations_by_deviceid,
#                  regions,
#                  districts)
# variables = {"form1": {1: {}, 2: {}, 3: {}, 4: {}}}
# for av in agg_variables:
#     variables["form1"][av.id][av.id] = Variable(av)



class ToCodeTest(unittest.TestCase):
    """
    Test the to_code functionality
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_location_information(self):
        """
        Testing that all the location infomation is translated correctly
        """
        row = {"index": 1, "column1": "A", "column2": "B34", "column3": "7",
                "date": "2015-10-25", "deviceid": 1, "meta/instanceID": "a"}
        result, alert = to_code(row, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.country, 1)
        self.assertEqual(result.region, 2)
        self.assertEqual(result.district, 4)
        self.assertEqual(result.clinic, 6)
        
        row["deviceid"] = 2
        result, alert = to_code(row, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.country, 1)
        self.assertEqual(result.region, 3)
        self.assertEqual(result.district, 5)
        self.assertEqual(alert.clinic, 7)

        row["deviceid"] = 3
        result, alert = to_code(row, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.country, 1)
        self.assertEqual(result.region, 2)
        self.assertEqual(result.district, None)
        self.assertEqual(alert.clinic, 8)
        row["deviceid"] = 99 
        result, alert = to_code(row, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result, None)

    def test_date(self):
        """
        Check that the date is handled properly
        """
        row = {"index": 1, "column1": "A", "column2": "B34", "column3": "7",
                "date": "2015-10-25", "deviceid": 1, "meta/instanceID": "a"}
        result, alert = to_code(row, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.date , datetime.datetime(2015, 10, 25))
        row["date"] = "Sep 6, 2015"
        result, alert = to_code(row, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.date , datetime.datetime(2015, 9, 6))

        # With a missing date column
        result, alert = to_code(row, variables, all_locations,
                                "another_date", "form1", alert_data)
        self.assertEqual(result, None)
        #With a non date
        row["date"] = "this is not a date"
        result, alert = to_code(row, variables, all_locations,
                                "another_date", "form1", alert_data)
        self.assertEqual(result, None)

    def test_variables(self):
        """
        Checking that variables returned and alerts are working
        """
        row1 = {"index": 1, "column1": "A", "column2": "B34", "column3": "7",
                "date": "2015-10-25", "deviceid": 1, "meta/instanceID": "a"}
        row2 = {"index": 2, "column1": "B", "column2": "A", "column3": "4",
                "date": "2015-10-25", "deviceid": 2, "meta/instanceID": "b"}
        row3 = {"index": 1, "column1": "A", "column2": "C", "column3": "7",
                "date": "2015-10-25", "deviceid": 2, "meta/instanceID": "c"}
        result, alert = to_code(row1, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.variables , {1: 1, 2: 1, 3: 1, 4: 1})
        self.assertEqual(alert.uuids , "a")
        self.assertEqual(alert.clinic , 6)
        self.assertEqual(alert.reason , 2)
        self.assertEqual(alert.date , datetime.datetime(2015, 10, 25))
        self.assertEqual(alert.data , {"column1": "A"})

        result, alert = to_code(row2, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.variables , {1: 1})
        self.assertEqual(alert,  None)

        result, alert = to_code(row3, variables, all_locations,
                                "date", "form1", alert_data)
        self.assertEqual(result.variables , {1: 1, 2: 1, 4: 1})
        self.assertEqual(alert.uuids , "c")
        self.assertEqual(alert.reason , 2)
        self.assertEqual(alert.date , datetime.datetime(2015, 10, 25))
        self.assertEqual(alert.data , {"column1": "A"})

if __name__ == "__main__":
    unittest.main()
