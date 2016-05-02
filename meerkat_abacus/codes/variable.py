"""
Definition of the Variable class


"""

from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

import meerkat_abacus.model as model
from meerkat_abacus.model import form_tables


class Variable():
    """
    A class for variables such that one can check if a row of data matches the variable

    A variable can have one of many methods. Each method has a different function that specifies how we determine if a row
    matches the variable. In the constructor we set test_type = method_specific_test. 

    Running variable.test(row, value) determins if the row matches the variable. 

    Variables can also have a secondary condition which is tested by the variable.secondary_condition(row, value) function. 


    We can have the following variable methods: 

    * count - Counts all rows with non-zero entry in the specified field of the form
    * count_occurrence - Counts rows where condtion appears in field
    * count_occurrence_in - Counts rows where condition is a substring of the value in the field
    * int_between - An integer between the two numbers specified in condition
    * count_occurrence_int_between - must both fullfill a count_occurrence and a int_between on two different columns
    * count_occurrence_in_int_between - must both fullfill a count_occurrence_in and a int_between on two different columns
    * sum - Returns the numerical value of the field
    * not_null - true for non-null values of the field
    * calc_between - allows you to specify a mathematical expression of multiple columns in the row. 
           The calculated value should then be between the given boundaries

    """
    def __init__(self, variable):
        """
        Set up variable class. We prepare the conditions/boundaries and determine the correct test function. 
        
        Args: 
            variable: model.AggregationVariable object
        """
        self.variable = variable
        self.column = variable.db_column
        if variable.method == "count_occurrence":
            if "," in variable.condition:
                self.cond_list = variable.condition.split(",")
                self.cond_list = [cond.strip() for cond in self.cond_list]
                self.test_type = self.test_count_occurrence_list
            else:
                self.cond = variable.condition
                self.test_type = self.test_count_occurrence
        elif variable.method == "count":
            self.test_type = self.test_count
        elif variable.method == "count_occurrence_in":
            self.test_type = self.test_count_occurrence_in
            if "," in variable.condition:
                self.cond_list = variable.condition.split(",")
            else:
                self.cond_list = [variable.condition]
            self.cond_list = [cond.strip() for cond in self.cond_list]
        elif variable.method == "int_between":
            self.test_type = self.test_int_between
            self.condition = variable.condition.split(",")
        elif variable.method == "sum":
            self.test_type = self.test_sum
        elif variable.method == "count_occurrence,int_between":
            self.test_type = self.test_count_occurrence_int_between
            self.column1, self.column2 = variable.db_column.split(",")
            self.condition_low, self.condition_high = (
                variable.condition.split(":")[1].split(","))
            self.condition = variable.condition.split(":")[0]
            if "," in self.condition:
                self.cond_list = self.condition.split(",")
            else:
                self.cond_list = [self.condition]
            self.cond_list = [cond.strip() for cond in self.cond_list]
        elif variable.method == "count_occurrence_in,int_between":
            self.test_type = self.test_count_occurrence_in_int_between
            self.column1, self.column2 = variable.db_column.split(",")
            self.condition_low, self.condition_high = (
                variable.condition.split(":")[1].split(","))

            self.condition = variable.condition.split(":")[0]
            if "," in self.condition:
                self.cond_list = self.condition.split(",")
            else:
                self.cond_list = [self.condition]
            self.cond_list = [cond.strip() for cond in self.cond_list]
        elif variable.method == "not_null":
            self.test_type = self.test_not_null
        elif variable.method == "calc_between":
            columns, self.calc = self.column.split(";")
            self.columns = [c.strip() for c in columns.split(",")]
            self.condition_low, self.condition_high = (
                variable.condition.split(","))
            self.condition_low = float(self.condition_low)
            self.condition_high = float(self.condition_high)
            self.test_type = self.test_calc_between
        else:
            raise NameError("Variable does not have test type {}"
                            .format(variable.method))
        if variable.secondary_condition:
            self.secondary_condition = self.secondary_condition_test
            self.sec_column, self.sec_condition = variable.secondary_condition.split(":")
        else:
            self.secondary_condition = self.secondary_condition_no
            
    def secondary_condition_test(self, row):
        """
        Tests if secondary condition is fullfilled
        
        Args:
           row: db-row
        Return:
           result(Bool): result of test
        """
        if row.get(self.sec_column, "neppe") == self.sec_condition:
            return 1
        else:
            return 0
        
    def secondary_condition_no(self, row):
        """
        Returns 1 sicne the variable does not have a secondary condition
        Args:
           row: db-row
        Return:
           result(Bool): 1

        """
        return 1

    def test(self, row, value):
        """
        Tests if current variable is true for row

        Args:
            row: a row from a form

        Returns:
            id(int): 0 if false and 1 (or sum) if true
        """
        return self.test_type(row, value)

    def test_count_occurrence_list(self, row, value):
        """Test if value is in condition list"""
        if value in self.cond_list:
            return 1
        else:
            return 0

    def test_count_occurrence(self, row, value):
        """Test if value==condition"""
        if value == self.cond:
            return 1
        else:
            return 0

    def test_count(self, row, value):
        """Returns 1 as long as value is not None"""
        if value is not None:
            return 1
        else:
            return 0

    def test_count_occurrence_in(self, row, value):
        """
        We first test if value is in the list, if not we check if value is a substring of any element in the list
        """
        column = self.column
        add = 0
        if row.get(column, None) in self.cond_list:
            add = 1
        else:
            for c in self.cond_list:
                if row.get(column, None) and c in row.get(column, None):
                    add = 1
                    break
        return add

    def test_int_between(self, row, value):
        """Test that condtion_lower<=value<condition_upper"""
        column = self.column
        add = 0
        condition_low, condition_high = self.condition
        if value or (condition_low == "0" and value != "" and int(value) == 0):
            n = int(float(row.get(column, -99999)))
            if n >= int(condition_low) and n < int(condition_high):
                add = 1
        return add

    def test_not_null(self, row, value):
        """ Value not equal None"""
        if value is not "" and value is not None:
            return value
        else:
            return 0
        
    def test_count_occurrence_int_between(self, row, value):
        """test both count_occurrence and int_between"""
        column2 = self.column2
        add = 0
        if (row.get(column2, None) and row.get(column2, None) != 0 or
            (self.condition_low == 0 and row.get(column2, None) == 0)):
            n = int(row.get(column2, None))
            if n >= int(self.condition_low) and n < int(self.condition_high):
                if row[self.column1] in self.cond_list:
                    add = 1
        return add

    def test_count_occurrence_in_int_between(self, row, value):
        """test both count_occurrence_in and int_between"""
        column2 = self.column2
        add = 0
        if (row.get(column2, None) and row.get(column2, None) != 0 or
            (self.condition_low == 0 and row.get(column2, None) == 0)):
            n = int(row.get(column2, None))
            if n >= int(self.condition_low) and n < int(self.condition_high):
                column1 = self.column1
                if row[column1] in self.cond_list:
                    add = 1
                else:
                    for c in self.cond_list:
                        if row[column1] and c in row[column1]:
                            add = 1
                            break
        return add

    def test_sum(self, row, value):
        """ Returns the value if it is non-None"""
        value = row.get(self.column, 0)
        if value:
            return int(value)
        else:
            return 0

    def test_calc_between(self, row, value):
        """
        self. calc should be an expression with column names from the row and mathematical expression 
        understood by python. We then replace all column names with their numerical values and evalualte
        the resulting expression. 

        """
        calc = self.calc
        for c in self.columns:
            if c in row and row[c]:
                calc = calc.replace(c, str(float(row[c])))
            else:
                return 0
        result = eval(calc)
        if self.condition_low <= result and self.condition_high > result:
            return 1
        else:
            return 0

