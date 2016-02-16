import unittest
import datetime

from meerkat_abacus import model
from meerkat_abacus.codes.to_codes import to_code
from meerkat_abacus.codes.variable import Variable

class TocodeTest(unittest.TestCase):
    """
    Test setting up database functionality
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_count(self):
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
                                        parent_location=5)
                     }
        locations_by_deviceid = {1: 6, 2: 7}
        regions = [2, 3]
        districts = [4, 5]

        agg_variables = [
            model.AggregationVariables(
                id=1,
                secondary_condition="",
                method="count",
                db_column="index",
                form="form1"),
            model.AggregationVariables(
                id=2,
                secondary_condition="",
                method="count_occurence",
                db_column="column1",
                alert=1,
                condition="A",
                form="form1"),
            model.AggregationVariables(
                id=3,
                secondary_condition="",
                method="count_occurence_in",
                db_column="column2",
                condition="B",
                form="form1"),
            model.AggregationVariables(
                id=4,
                secondary_condition="",
                method="int_between",
                db_column="column3",
                condition="5,10",
                form="form1")
        ]
        alert_data = {"column1": "column1"}
        all_locations = (locations,
                         locations_by_deviceid,
                         regions,
                         districts)
        variables = {"form1": {1: {}, 2: {}, 3: {}, 4: {}}}
        for av in agg_variables:
            variables["form1"][av.id][av.id] = Variable(av)

        row1 = {"index": 1, "column1": "A", "column2": "B34", "column3": "7",
                "date": "2015-10-25", "deviceid": 1, "meta/instanceID": "a"}
        row2 = {"index": 2, "column1": "B", "column2": "A", "column3": "4",
                "date": "2015-10-25", "deviceid": 2, "meta/instanceID": "a"}
        row3 = {"index": 1, "column1": "A", "column2": "C", "column3": "7",
                "date": "2015-10-25", "deviceid": 2, "meta/instanceID": "a"}
        result, alert = to_code(row1, variables, all_locations,
                                "date", "form1", alert_data)
        print(result.variables)
        assert result.variables == {1: 1, 2: 1, 3: 1, 4: 1}
        assert result.country == 1
        assert result.region == 2
        assert result.district == 4
        assert result.clinic == 6
        assert result.date == datetime.datetime(2015, 10, 25)
        assert alert.uuids == "a"
        assert alert.clinic == 6
        assert alert.reason == 2
        assert alert.date == datetime.datetime(2015, 10, 25)
        assert alert.data == {"column1": "A"}
        result, alert = to_code(row2, variables, all_locations,
                                "date", "form1", alert_data)
        assert result.variables == {1: 1}
        assert result.country == 1
        assert result.region == 3
        assert result.district == 5
        assert alert is None
        result, alert = to_code(row3, variables, all_locations,
                                "date", "form1", alert_data)
        assert result.variables == {1: 1, 2: 1, 4: 1}
        assert alert.uuids == "a"
        assert alert.clinic == 7
        assert alert.reason == 2
        assert alert.date == datetime.datetime(2015, 10, 25)
        assert alert.data == {"column1": "A"}

if __name__ == "__main__":
    unittest.main()
