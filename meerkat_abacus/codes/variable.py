"""
Definition of the Variable class


"""
from functools import partial, partialmethod
from sympy import sympify
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
import logging
import meerkat_abacus.model as model
from meerkat_abacus.model import form_tables


class Variable():
    """
    A class for variables such that one can check if a row of data matches the variable

    """
    def __init__(self, variable):
        """
        Set up variable class. We prepare the conditions/boundaries and determine the correct test function. 
        
        Args: 
            variable: model.AggregationVariable object
        """
        self.variable = variable
        self.column = variable.db_column
        self.operations = []
        self.test_types = []
        i = 0
        self.bool_expression = ""
        self.bool_variables = []
        bool_trans = {"and": "&", "or": "|"}
        for term in variable.method.split(" "):
            if i % 2 == 0:
                if term in ["match", "sub_match", "between", "value", "not_null", "calc"]:
                    self.test_types.append(term)
                else:
                    raise NameError("Wrong test type")
                var = chr(97 + i)
                self.bool_expression += var
                self.bool_variables.append(var)
            else:
                if term in ["and", "or", "not"]:
                    self.operations.append(term)
                    self.bool_expression += bool_trans[term]
                else:
                    raise NameError("Wrong logic type")
            i += 1

        self.conditions = []
        for condition in variable.condition.split(";"):
            if "," in condition:
                c = [c.strip() for c in condition.split(",")]
            else:
                c = [condition]
            self.conditions.append(c)
        self.columns = []
        for column in variable.db_column.split(";"):
            if "," in column:
                c = [c.strip() for c in column.split(",")]
            else:
                c = column
            self.columns.append(c)
        if len(self.conditions) != len(self.test_types):
            raise TypeError("Need same number of conditions as test types, {}".format(variable))
        self.test_functions = {
            "match": self.test_match,
            "sub_match": self.test_sub_match,
            "between": self.test_calc_between,
            "not_null": self.test_not_null
        }

        if "value" in self.test_types:
            if len(self.test_types) > 1:
                raise NameError("Value must be only test type")
            self.test_type = self.test_value
        elif "calc" in self.test_types:
            if len(self.test_types) > 1:
                raise NameError("calc must be only test_type")
            self.calculation = variable.calculation
            if not isinstance(self.columns[0], list):
                self.columns[0] = [self.columns[0]]
            self.test_type = self.test_calc
        elif len(self.test_types) == 1:
            tt = self.test_types[0]
            if tt == "match":
                self.test_type = partial(self.test_match,
                                               self.columns[0],
                                               self.conditions[0])
            elif tt == "sub_match":
                self.test_type = partial(self.test_sub_match,
                                               self.columns[0], self.conditions[0])
            elif tt == "between":
                self.test_type = partial(self.test_calc_between,
                                               self.columns[0],
                                               self.conditions[0],
                                               variable.calculation)
            else:
                self.test_type = self.test_functions[self.test_types[0]]
        else:
            if hasattr(variable, "calculation") and variable.calculation:
                self.calculation = variable.calculation.split(";")
            self.test_type = self.test_many

    def test(self, row, value):
        """
        Tests if current variable is true for row

        Args:
            row: a row from a form

        Returns:
            id(int): 0 if false and 1 (or sum) if true
        """
        return self.test_type(row, value)

    def test_many(self, row, value):

        res_dict = {}
        for i in range(len(self.test_types)):
            tt = self.test_types[i]
            if tt == "match":
                res = self.test_match(self.columns[i], self.conditions[i], row, value)
            elif tt == "sub_match":
                res = self.test_sub_match(self.columns[i], self.conditions[i], row, value)
            elif tt == "between":
                if not isinstance(self.columns[i], list):
                    self.columns[i] = [self.columns[i]]
                

                res = self.test_calc_between(self.columns[i],
                                             self.conditions[i],
                                             self.calculation[i],
                                             row, value)
            else:
                res = self.test_functions[self.test_types[i]](row, value)
            res_dict[self.bool_variables[i]] = res
        r = sympify(self.bool_expression).subs(res_dict)

        if r:
            return 1
        else:
            return 0
        
    def test_match(self, column, condition, row, value):
        """Test if value is in condition list"""
        if row.get(column, None) in condition:
            return 1
        else:
            return 0

    def test_sub_match(self, column, condition, row, value):
        """
        We first test if value is in the list, if not we check if value is a substring of any element in the list
        """

        add = 0
        if row.get(column, None) in condition:
            add = 1
        else:
            for c in condition:
                if row.get(column, None) and c in row.get(column, None):
                    add = 1
                    break
        return add

    def test_not_null(self, row, value):
        """ Value not equal None"""
        if value is not "" and value is not None and value is not 0:
            return 1
        else:
            return 0
    def test_value(self, row, value):
        """ Value not equal None"""
        if value is not "" and value is not None and value is not 0:
            return value
        else:
            return 0

    def test_calc_between(self, columns, condition, calc, row, value,):
        """
        self. calc should be an expression with column names from the row and mathematical expression 
        understood by python. We then replace all column names with their numerical values and evalualte
        the resulting expression. 

        """

        for c in columns:
            if c in row and row[c]:
                calc = calc.replace(c, str(float(row[c])))
            else:
                return 0
        result = eval(calc)
        if float(condition[0]) <= result and float(condition[1]) > result:
            return 1
        else:
            return 0

    def test_calc(self, row, value):
        """
        self. calc should be an expression with column names from the row and mathematical expression 
        understood by python. We then replace all column names with their numerical values and evalualte
        the resulting expression. 

        """

        calc = self.calculation
        for c in self.columns[0]:
            if c in row and row[c]:
                calc = calc.replace(c, str(float(row[c])))
            else:
                calc = calc.replace(c, "0")      
 
        return int(eval(calc))

        # if variable.method == "count_occurrence":
        #     if "," in variable.condition:
        #         self.cond_list = variable.condition.split(",")
        #         self.cond_list = [cond.strip() for cond in self.cond_list]
        #         self.test_type = self.test_count_occurrence_list
        #     else:
        #         self.cond = variable.condition
        #         self.test_type = self.test_count_occurrence
        # elif variable.method == "count":
        #     self.test_type = self.test_count
        # elif variable.method == "count_occurrence_in":
        #     self.test_type = self.test_count_occurrence_in
        #     if "," in variable.condition:
        #         self.cond_list = variable.condition.split(",")
        #     else:
        #         self.cond_list = [variable.condition]
        #     self.cond_list = [cond.strip() for cond in self.cond_list]
        # elif variable.method == "int_between":
        #     if ";" in variable.condition:
        #         if "," in variable.db_column:
        #             self.test_type = self.test_int_between_multiple
        #             self.condition = []
        #             for cond in variable.condition.split(";"):
        #                 self.condition.append(cond.split(","))
        #             self.column_list = self.column.split(",")
        #             if len(self.column_list) != len(self.condition):
        #                 raise KeyError("Needs same number of db columns as conditions")
        #         else:
        #             raise KeyError("Needs same number of db columns as conditions")
        #     else:
        #         self.test_type = self.test_int_between
        #         self.condition = variable.condition.split(",")
            
        # elif variable.method == "sum":
        #     self.test_type = self.test_sum
        # elif variable.method == "count_occurrence,int_between":
        #     self.test_type = self.test_count_occurrence_int_between
        #     self.column1, self.column2 = variable.db_column.split(",")
        #     self.condition_low, self.condition_high = (
        #         variable.condition.split(":")[1].split(","))
        #     self.condition = variable.condition.split(":")[0]
        #     if "," in self.condition:
        #         self.cond_list = self.condition.split(",")
        #     else:
        #         self.cond_list = [self.condition]
        #     self.cond_list = [cond.strip() for cond in self.cond_list]
        # elif variable.method == "count_occurrence_in,int_between":
        #     self.test_type = self.test_count_occurrence_in_int_between
        #     self.column1, self.column2 = variable.db_column.split(",")
        #     self.condition_low, self.condition_high = (
        #         variable.condition.split(":")[1].split(","))

        #     self.condition = variable.condition.split(":")[0]
        #     if "," in self.condition:
        #         self.cond_list = self.condition.split(",")
        #     else:
        #         self.cond_list = [self.condition]
        #     self.cond_list = [cond.strip() for cond in self.cond_list]

        # elif variable.method == "count_or_occurrence,int_between":
        #     self.test_type = self.test_count_or_occurrence_int_between
 
        #     self.column1, self.column2 = variable.db_column.split(",")[0].split(" ")
        #     self.column = variable.db_column.split(",")[1]
            
        #     self.cond_one, self.cond_two = variable.condition.split(":")[0].split(",")
        #     self.condition = variable.condition.split(":")[1].split(",")

        # elif variable.method == "count_or_occurrence":
        #     self.test_type = self.test_count_or_occurrence
        #     self.column1, self.column2 = variable.db_column.split(" ")
        #     self.cond_one, self.cond_two = variable.condition.split(",")
        # elif variable.method == "count_and_occurrence":
        #     self.test_type = self.test_count_and_occurrence
        #     self.column1, self.column2 = variable.db_column.split(" ")
        #     self.cond_one, self.cond_two = variable.condition.split(",")
        # elif variable.method == "not_null":
        #     if "," in variable.db_column:
        #         self.columns = variable.db_column.split(",")
        #         print(self.columns)
        #         self.test_type = self.test_not_null_many
        #     else:
        #         self.test_type = self.test_not_null
        # elif variable.method == "take_value":
        #     self.test_type = self.test_take_value
        # elif variable.method == "calc_between":
        #     columns, self.calc = self.column.split(";")
        #     self.columns = [c.strip() for c in columns.split(",")]
        #     self.condition_low, self.condition_high = (
        #         variable.condition.split(","))
        #     self.condition_low = float(self.condition_low)
        #     self.condition_high = float(self.condition_high)
        #     self.test_type = self.test_calc_between
        # elif variable.method == "calc":
        #     columns, self.calc = self.column.split(";")
        #     self.columns = [c.strip() for c in columns.split(",")]
        #     self.test_type = self.test_calc
        # else:
        #     raise NameError("Variable does not have test type {}"
        #                     .format(variable.method))
        # if variable.secondary_condition:
        #     self.secondary_condition = self.secondary_condition_test
        #     self.sec_column, self.sec_condition = variable.secondary_condition.split(":")
        # else:
        #     self.secondary_condition = self.secondary_condition_no
            
