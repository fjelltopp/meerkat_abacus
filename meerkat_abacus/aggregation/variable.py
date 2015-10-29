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
            self.test_type = self.test_count_occurence
            if "," in variable.condition:
                self.cond_list = variable.condition.split(",")
            else:
                self.cond_list = [variable.condition]
                if variable.condition == "1":
                    self.cond_list = [1, "1"]
            self.cond_list = [cond.strip() for cond in self.cond_list]
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


    def test(self, row):
        """
        Tests if current variable is true for row

        Args:
        row: a row from a form

        Returns:
        id: 0 if false and id if true
        """
        variable = self.variable
        if variable.secondary_condition:
            sec_column, sec_condition = variable.secondary_condition.split(":")
            if row[sec_column] != sec_condition:
                return 0
        if self.test_type(row):
            return self.variable.id
        else:
            return 0

    def test_count_occurence(self, row):
        return row[self.column] in self.cond_list

    def test_count(self, row):
        return row[self.column] != None

    def test_count_occurence_in(self, row):
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

    def test_int_between(self, row):
        column = self.column
        add = 0
        condition_low, condition_high = self.condition
        if ((row[column] and row[column] != 0) or
            (condition_low == "0" and int(row[column]) == 0)):
            n = int(row[column])
            if n >= int(condition_low) and n < int(condition_high):
                add = 1
        return add

    def test_count_occurence_int_between(self, row):
        column2 = self.column2
        add = 0
        if (row[column2] and row[column2] != 0 or
            (self.condition_low == 0 and row[column2] == 0)):
            n = int(row[column2])
            if n >= int(self.condition_low) and n < int(self.condition_high):
                if row[self.column1] in self.condition:
                    add = 1
        return add

    def test_count_occurence_in_int_between(self, row):
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
