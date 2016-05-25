import unittest

from meerkat_abacus import model
from meerkat_abacus.codes.variable import Variable


class VariableTest(unittest.TestCase):
    """
    Tests to check that Variables class gives the correct results on test cases
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_count(self):
        """
        testing the count method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": 1}
        self.assertEqual(variable.test(row, row["index"]), 1)
        row = {"index": 3}
        self.assertEqual(variable.test(row, row["index"]), 1)
        row = {"index": None}
        self.assertEqual(variable.test(row, row["index"]), 0)
        
    def test_not_null(self):
        """
        testing the not_null method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="not_null",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": "hei"}
        self.assertEqual(variable.test(row, row["index"]), "hei")
        row = {"index": ""}
        self.assertEqual(variable.test(row, row["index"]), 0)
        row = {"index": 0}
        self.assertEqual(variable.test(row, row["index"]), 0)
        row = {"index": None}
        self.assertEqual(variable.test(row, row["index"]), 0)

    def test_calc_between(self):
        """
        testing the calc_between method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="calc_between",
            condition="0,1",
            db_column="A,B;A**2/(B-4)")
        variable = Variable(agg_variable)
        row = {"A": "1", "B": "6"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"A": "2", "B": "6"}
        self.assertEqual(variable.test(row, None), 0)
        row = {"A": "2", "B": "400"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"A": "2", "B": "1"}
        self.assertEqual(variable.test(row, None), 0)

        row = {"A": "2"} # test if column is missing
        self.assertEqual(variable.test(row, None), 0)
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="calc_between",
            condition="0,1",
            db_column="A,B;C/(B-4)")
        # note we have used C which is not one of the columns, so the test should give an error
        variable = Variable(agg_variable)
        row = {"A": "2", "B": "6"}
        with self.assertRaises(NameError):
            variable.test(row, None)

    def test_calc_between(self):
        """
        testing the calc_between method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="calc",
            condition="",
            db_column="A,B;A+B")
        variable = Variable(agg_variable)

        row = {"A": "1", "B": "6"}
        self.assertEqual(variable.test(row, None), 7)
        row = {"A": "2", "B": "400"}
        self.assertEqual(variable.test(row, None), 402)
        row = {"A": "2"} # test if column is missing
        self.assertEqual(variable.test(row, None), 2)

        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="calc",
            condition="",
            db_column="A,B;A+C")
        # note we have used C which is not one of the columns, so the test should give an error
        variable = Variable(agg_variable)
        row = {"A": "2", "B": "6"}
        with self.assertRaises(NameError):
            variable.test(row, None)      

    def test_count_occurrence(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurrence",
            db_column="column1",
            condition="A")
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        row = {"column1": "Aa"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        agg_variable.condition = "A,C"
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "C"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        row = {"column1": "Aa"}
        self.assertEqual(variable.test(row, row["column1"]), 0)

    def test_count_or_occurrence(self):

        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_or_occurrence",
            db_column="column1,column2",
            condition="A,B")
        variable = Variable(agg_variable)

        row = {"column1": "A"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        row = {"column1": "C"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        row = {"column2": "A"}
        self.assertEqual(variable.test(row, row["column2"]), 0)
        row = {"column2": "B"}
        self.assertEqual(variable.test(row, row["column2"]), 1)
        row = {"column2": "C"}
        self.assertEqual(variable.test(row, row["column2"]), 0)
        row = {"column3": "A"}
        self.assertEqual(variable.test(row, row["column3"]), 0)
        row = {"column3": "B"}
        self.assertEqual(variable.test(row, row["column3"]), 0)
        
    def test_int_between(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="int_between",
            db_column="column1",
            condition="3,6")
        variable = Variable(agg_variable)
        row = {"column1": "3"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "5"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "9"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        row = {"column1": "6"}
        self.assertEqual(variable.test(row, row["column1"]), 0)
        agg_variable.condition = "0,5"
        variable = Variable(agg_variable)
        row = {"column1": "0"}
        self.assertEqual(variable.test(row, row["column1"]), 1)

    def test_count_occerence_in(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurrence_in",
            db_column="column1",
            condition="A")
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "A3"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row, row["column1"]), 0)

        agg_variable.condition = "A,C"
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "C"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "A1"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "C3"}
        self.assertEqual(variable.test(row, row["column1"]), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row, row["column1"]), 0)

    def test_no_such_method(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="no_such_method",
            db_column="column1",
            condition="A")
        with self.assertRaises(NameError):
            variable = Variable(agg_variable)
        
    def test_count_occurrence_int_between(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurrence,int_between",
            db_column="column2,column1",
            condition="AB:0,5")
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "AB"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"column1": "7", "column2": "AB"}
        self.assertEqual(variable.test(row, None), 0)
        row = {"column1": "3", "column2": "B"}
        self.assertEqual(variable.test(row, None), 0)
        row = {"column1": "0", "column2": "AB"}
        self.assertEqual(variable.test(row, None), 1)
        agg_variable.condition = "AB,C:0,5"
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "AB"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"column1": "3", "column2": "C"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"column1": "3", "column2": "B"}
        self.assertEqual(variable.test(row, None), 0)
        row = {"column1": "3", "column2": "B"}
        self.assertEqual(variable.test(row, None), 0)

    def test_count_occurrence_in_int_between(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count_occurrence_in,int_between",
            db_column="column2,column1",
            condition="A:0,5")
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "Aa.3"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"column1": "7", "column2": "A"}
        self.assertEqual(variable.test(row, None), 0)
        row = {"column1": "3", "column2": "B"}
        self.assertEqual(variable.test(row, None), 0)
        row = {"column1": "0", "column2": "A"}
        self.assertEqual(variable.test(row, None), 1)
        agg_variable.condition = "A,C:0,5"
        variable = Variable(agg_variable)
        row = {"column1": "3", "column2": "A"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"column1": "3", "column2": "C"}
        self.assertEqual(variable.test(row, None), 1)
        row = {"column1": "3", "column2": "B"}
        self.assertEqual(variable.test(row, None), 0)

    def test_secondary_condition(self):
        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="",
            method="count",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": 1, "column2": "A"}
        self.assertEqual(variable.secondary_condition(row), 1)

        agg_variable = model.AggregationVariables(
            id=4,
            secondary_condition="column2:A",
            method="count",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": 1, "column2": "A"}
        self.assertEqual(variable.secondary_condition(row), 1)
        row = {"index": 1, "column2": "B"}
        self.assertEqual(variable.secondary_condition(row), 0)
        
    def test_sum(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="sum",
            db_column="column1")
        variable = Variable(agg_variable)
        row = {"column1": ""}
        self.assertEqual(variable.test(row, None), 0)
        row = {"column1": "4"}
        self.assertEqual(variable.test(row, 4), 4)


if __name__ == "__main__":
    unittest.main()
