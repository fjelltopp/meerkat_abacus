"""
Functionality to translate raw data into codes
specified by codes file
"""

from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

import meerkat_abacus.model as model
from meerkat_abacus.model import form_tables


class Variable():
    """
    A class for storing variables for use in checking records
    """
    def __init__(self, variable):
        """
        Store variable dictionary
        
        Args: 
            variable: model.AggregationVariable object
        """
        self.variable = variable
        self.column = variable.db_column
        if variable.method == "count_occurence":

            if "," in variable.condition:
                self.cond_list = variable.condition.split(",")
                self.cond_list = [cond.strip() for cond in self.cond_list]
                self.test_type = self.test_count_occurence_list
            else:
                self.cond = variable.condition
                self.test_type = self.test_count_occurence
        elif variable.method == "count":
            self.test_type = self.test_count
        elif variable.method == "count_occurence_in":
            self.test_type = self.test_count_occurence_in
            if "," in variable.condition:
                self.cond_list = variable.condition.split(",")
            else:
                self.cond_list = [variable.condition]
                if variable.condition == "1":
                    self.cond_list = [1, "1"]
            self.cond_list = [cond.strip() for cond in self.cond_list]
        elif variable.method == "int_between":
            self.test_type = self.test_int_between
            self.condition = variable.condition.split(",")
        elif variable.method == "sum":
            self.test_type = self.test_sum
        elif variable.method == "count_occurence,int_between":
            self.test_type = self.test_count_occurence_int_between
            self.column1, self.column2 = variable.db_column.split(",")
            self.condition_low, self.condition_high = (
                variable.condition.split(":")[1].split(","))
            self.condition = variable.condition.split(":")[0]
            if "," in self.condition:
                self.cond_list = self.condition.split(",")
            else:
                self.cond_list = [self.condition]
                if self.condition == "1":
                    self.cond_list = [1, "1"]
            self.cond_list = [cond.strip() for cond in self.cond_list]
        elif variable.method == "count_occurence_in,int_between":
            self.test_type = self.test_count_occurence_in_int_between
            self.column1, self.column2 = variable.db_column.split(",")
            self.condition_low, self.condition_high = (
                variable.condition.split(":")[1].split(","))

            self.condition = variable.condition.split(":")[0]
            if "," in self.condition:
                self.cond_list = self.condition.split(",")
            else:
                self.cond_list = [self.condition]
                if self.condition == "1":
                    self.cond_list = [1, "1"]
            self.cond_list = [cond.strip() for cond in self.cond_list]
            
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
        """
        if row.get(self.sec_column, "neppe") == self.sec_condition:
            return 1
        else:
            return 0
        
    def secondary_condition_no(self, row):
        """
        Returns 1 sicne the variable does not have a secondary condition
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

    def test_count_occurence_list(self, row, value):
        return value in self.cond_list

    def test_count_occurence(self, row, value):
        return value == self.cond

    def test_count(self, row, value):
        return value != None

    def test_count_occurence_in(self, row, value):
        column = self.column
        add = 0
        if row[column] in self.cond_list:
            add = 1
        else:
            for c in self.cond_list:
                if row[column] and c in row[column]:
                    add = 1
                    break
        return add

    def test_int_between(self, row, value):
        column = self.column
        add = 0
        condition_low, condition_high = self.condition
        if ((row[column] and row[column] != 0) or
            (condition_low == "0" and int(row[column]) == 0)):
            n = int(row[column])
            if n >= int(condition_low) and n < int(condition_high):
                add = 1
        return add

    def test_count_occurence_int_between(self, row, value):
        column2 = self.column2
        add = 0
        if (row[column2] and row[column2] != 0 or
            (self.condition_low == 0 and row[column2] == 0)):
            n = int(row[column2])
            if n >= int(self.condition_low) and n < int(self.condition_high):
                if row[self.column1] in self.cond_list:
                    add = 1
        return add

    def test_count_occurence_in_int_between(self, row, value):
        column2 = self.column2
        add = 0
        if (row[column2] and row[column2] != 0 or
            (self.condition_low == 0 and row[column2] == 0)):
            n = int(row[column2])
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
        column = self.column
        if row[column]:
            return int(row[column])
        else:
            return 0
