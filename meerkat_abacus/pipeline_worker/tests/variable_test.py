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

        
    def test_not_null(self):
        """
        testing the not_null method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            method="not_null",
            condition="None",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": "hei"}
        self.assertEqual(variable.test(row), 1)
        row = {"index": ""}
        self.assertEqual(variable.test(row), 0)
        row = {"index": 0}
        self.assertEqual(variable.test(row), 0)
        row = {"index": None}
        self.assertEqual(variable.test(row), 0)

    def test_value(self):
        """
        testing the not_null method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            method="value",
            condition="None",
            db_column="index")
        variable = Variable(agg_variable)
        row = {"index": "hei"}
        self.assertEqual(variable.test(row), "hei")
        row = {"index": ""}
        self.assertEqual(variable.test(row), 0)
        row = {"index": 0}
        self.assertEqual(variable.test(row), 0)
        row = {"index": None}
        self.assertEqual(variable.test(row), 0)
        
    def test_between(self):
        """
        testing the between method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            method="between",
            condition="0,1",
            calculation="A**2/(B-4)",
            db_column="A,B")
        variable = Variable(agg_variable)
        row = {"A": "1", "B": "6"}
        self.assertEqual(variable.test(row), 1)
        row = {"A": "2", "B": "6"}
        self.assertEqual(variable.test(row), 0)
        row = {"A": "2", "B": "400"}
        self.assertEqual(variable.test(row), 1)
        row = {"A": "2", "B": "1"}
        self.assertEqual(variable.test(row), 0)

        row = {"A": "2"} # test if column is missing
        self.assertEqual(variable.test(row), 0)
        agg_variable = model.AggregationVariables(
            id=4,
            method="between",
            condition="0,1",
            calculation="C/(B-4)",
            db_column="A,B")
        # note we have used C which is not one of the columns, so the test should give an error
        variable = Variable(agg_variable)
        row = {"A": "2", "B": "6"}
        with self.assertRaises(NameError):
            variable.test(row)


        # Test with date
        agg_variable = model.AggregationVariables(
            id=4,
            method="between",
            condition="1388527200,2019679200", # 2014-2034
            calculation="Variable.to_date(A)",
            db_column="A")
        variable = Variable(agg_variable)
        row = {"A": "01-Jan-2016"}
        self.assertEqual(variable.test(row), 1)
        row = {"A": "01-Jan-2035"}
        self.assertEqual(variable.test(row), 0)
        row = {"A": "01-Jan-2010"}
        self.assertEqual(variable.test(row), 0)
        
    def test_calc(self):
        """
        testing the calc_between method
        """
        agg_variable = model.AggregationVariables(
            id=4,
            method="calc",
            condition="None",
            calculation="A+B",
            db_column="A,B")
        variable = Variable(agg_variable)

        row = {"A": "1", "B": "6"}
        self.assertEqual(variable.test(row), 7)
        row = {"A": "2", "B": "400"}
        self.assertEqual(variable.test(row), 402)
        row = {"A": "2"}  # test if column is missing
        self.assertEqual(variable.test(row), 0)

        agg_variable = model.AggregationVariables(
            id=4,
            method="calc",
            condition="None",
            calculation="A+C",
            db_column="A,B")
        # note we have used C which is not one of the columns, so the test should give an error
        variable = Variable(agg_variable)
        row = {"A": "2", "B": "6"}
        with self.assertRaises(NameError):
            variable.test(row)      

    def test_match(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="match",
            db_column="column1",
            condition="A")
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa"}
        self.assertEqual(variable.test(row), 0)
        agg_variable.condition = "A,C"
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "C"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa"}
        self.assertEqual(variable.test(row), 0)

    def test_sub_match(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="sub_match",
            db_column="column1",
            condition="A")
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "A3"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row), 0)

        agg_variable.condition = "A,C"
        variable = Variable(agg_variable)
        row = {"column1": "A"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "C"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "A1"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "C3"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B"}
        self.assertEqual(variable.test(row), 0)
        
    def test_and(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="match and match",
            db_column="column1;column2",
            condition="A;B")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "B"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B", "column2": "A"}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa", "column2": "B"}
        self.assertEqual(variable.test(row), 0)
        agg_variable = model.AggregationVariables(
            id=4,
            method="match and match",
            db_column="column1;column2",
            condition="A,C;B")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "B"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "C", "column2": "B"}
        self.assertEqual(variable.test(row), 1)

        
    def test_or(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="match or match",
            db_column="column1;column2",
            condition="A;B")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "B"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B", "column2": "A"}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa", "column2": "B"}
        self.assertEqual(variable.test(row), 1)

        row = {"column1": "Aa", "column2": "C"}
        self.assertEqual(variable.test(row), 0)

        agg_variable = model.AggregationVariables(
            id=4,
            method="match or match",
            db_column="column1;column2",
            condition="A,C;B")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "B"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "C", "column2": "D"}
        self.assertEqual(variable.test(row), 1)

    def test_different_test_types(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="match and sub_match",
            db_column="column1;column2",
            condition="A;B")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "Bb"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B", "column2": "A"}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa", "column2": "B"}
        self.assertEqual(variable.test(row), 0)

        agg_variable = model.AggregationVariables(
            id=4,
            method="match and between",
            db_column="column1;column2",
            calculation="None;column2",
            condition="A;4,9")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "5"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "A", "column2": "3"}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa", "column2": "5"}
        self.assertEqual(variable.test(row), 0)
        agg_variable = model.AggregationVariables(
            id=4,
            method="sub_match or not_null",
            db_column="column1;column2",
            condition="A;None")
        variable = Variable(agg_variable)
        row = {"column1": "A", "column2": "5"}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "A", "column2": ""}
        self.assertEqual(variable.test(row), 1)
        row = {"column1": "B", "column2": ""}
        self.assertEqual(variable.test(row), 0)
        row = {"column1": "Aa", "column2": "5"}
        self.assertEqual(variable.test(row), 1)
    def test_no_such_method(self):
        agg_variable = model.AggregationVariables(
            id=4,
            method="no_such_method",
            db_column="column1",
            condition="A")
        with self.assertRaises(NameError):
            variable = Variable(agg_variable)
        


if __name__ == "__main__":
    unittest.main()
