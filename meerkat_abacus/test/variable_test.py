import unittest

from meerkat_abacus import model
from meerkat_abacus.aggregation.variable import Variable


class VariableTest(unittest.TestCase):
    """
    Test setting up database functionality
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_count(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": 1}
        assert variable.test(row) == 4
        row = {"index": None}
        assert variable.test(row) == 0

    def test_count_occurence(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurence",
            db_column="column1",
            condition="A")
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        assert variable.test(row) == 4
        row = {"column1": "B"}
        assert variable.test(row) == 0
        agg_variable.condition = "A,C"
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        assert variable.test(row) == 4
        row = {"column1": "C"}
        assert variable.test(row) == 4
        row = {"column1": "B"}
        assert variable.test(row) == 0
        
    def test_int_between(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="int_between",
            db_column="column1",
            condition="3,6")
        variable = Variable(agg_variable)
        row = {"column1": "3"}
        assert variable.test(row) == 4

        row = {"column1": "5"}
        assert variable.test(row) == 4
        row = {"column1": "9"}
        assert variable.test(row) == 0
        row = {"column1": "6"}
        assert variable.test(row) == 0
        agg_variable.condition = "0,5"
        variable = Variable(agg_variable)
        row = {"column1": "0"}
        assert variable.test(row) == 4

    def test_count_occerence_in(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurence_in",
            db_column="column1",
            condition="A")
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        assert variable.test(row) == 4
        row = {"column1": "A3"}
        assert variable.test(row) == 4
        row = {"column1": "B"}
        assert variable.test(row) == 0

        agg_variable.condition = "A,C"
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        assert variable.test(row) == 4
        row = {"column1": "C"}
        assert variable.test(row) == 4
        row = {"column1": "A1"}
        assert variable.test(row) == 4
        row = {"column1": "C3"}
        assert variable.test(row) == 4
        row = {"column1": "B"}
        assert variable.test(row) == 0

    def test_count_occurence_int_between(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurence,int_between",
            db_column="column2,column1",
            condition="A:0,5")
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "A"}
        assert variable.test(row) == 4
        row = {"column1": "7", "column2": "A"}
        assert variable.test(row) == 0
        row = {"column1": "3", "column2": "B"}
        assert variable.test(row) == 0
        row = {"column1": "0", "column2": "A"}
        assert variable.test(row) == 4
        agg_variable.condition = "A,C:0,5"
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "A"}
        assert variable.test(row) == 4
        row = {"column1": "3", "column2": "C"}
        assert variable.test(row) == 4
        row = {"column1": "3", "column2": "B"}
        assert variable.test(row) == 0

    def test_count_occurence_in_int_between(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurence_in,int_between",
            db_column="column1,column2",
            condition="A:0,5")
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "A"}
        assert variable.test(row) == 4
        row = {"column1": "7", "column2": "A"}
        assert variable.test(row) == 0
        row = {"column1": "3", "column2": "B"}
        assert variable.test(row) == 0
        row = {"column1": "0", "column2": "A"}
        assert variable.test(row) == 4
        agg_variable.condition = "A,C:0,5"
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "A"}
        assert variable.test(row) == 4
        row = {"column1": "3", "column2": "C"}
        assert variable.test(row) == 4
        row = {"column1": "3", "column2": "B"}
        assert variable.test(row) == 0

    def test_secondary_condition(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="column2:A",
            method="count",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": 1, "column2": "A"}
        assert variable.test(row) == 4
        row = {"index": 1, "column2": "B"}
        assert variable.test(row) == 0

        
if __name__ == "__main__":
    unittest.main()
