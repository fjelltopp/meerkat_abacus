"""
Definition of the Variable class


"""
from dateutil.parser import parse
from functools import partial

# from sympy import sympify


class Variable():
    """
    A class for variables such that one can check if a row of data
    matches the variable

    """

    def __init__(self, variable):
        """
        Set up variable class. We prepare the conditions/boundaries
        and determine the correct test function.
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
                if term in ["match", "sub_match", "between", "value",
                            "not_null", "calc"]:
                    self.test_types.append(term)
                else:
                    raise NameError("{} has wrong test type".format(variable.id))
                var = chr(97 + i)
                self.bool_expression += 'res_dict["' + var + '"]'
                self.bool_variables.append(var)
            else:
                if term in ["and", "or", "not"]:
                    self.operations.append(term)
                    self.bool_expression += bool_trans[term]
                else:
                    raise NameError("Wrong logic type")
            i += 1
        self.bool_expression = compile(self.bool_expression, "<string>",
                                       "eval")
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
            raise TypeError("Need same number of conditions as test types, {}".
                            format(variable))
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
            self.calculation = variable.calculation
            
        elif "calc" in self.test_types:
            if len(self.test_types) > 1:
                raise NameError("calc must be only test_type")
            self.calculation = variable.calculation
            if not isinstance(self.columns[0], list):
                self.columns[0] = [self.columns[0]]
            for c in self.columns[0]:
                self.calculation = self.calculation.replace(
                    c, 'float(row["' + c + '"])')
            self.calculation = compile(self.calculation, "<string>", "eval")

            self.test_type = self.test_calc

        elif len(self.test_types) == 1:
            tt = self.test_types[0]
            if tt == "match":
                self.test_type = partial(self.test_match, self.columns[0],
                                         self.conditions[0])
            elif tt == "sub_match":
                self.test_type = partial(self.test_sub_match, self.columns[0],
                                         self.conditions[0])
            elif tt == "between":
                if not isinstance(self.columns[0], list):
                    self.columns[0] = [self.columns[0]]

                self.calculation = variable.calculation
                for c in self.columns[0]:
                    self.calculation = self.calculation.replace(
                        c, 'float(row["' + c + '"])')
                self.calculation = compile(self.calculation, "<string>",
                                           "eval")
                self.test_type = partial(self.test_calc_between,
                                         self.columns[0], self.conditions[0],
                                         self.calculation)
            elif tt == "not_null":
                self.test_type = partial(self.test_not_null, self.columns[0])

            else:
                self.test_type = self.test_functions[self.test_types[0]]
        else:
            if hasattr(variable, "calculation") and variable.calculation:
                self.calculation = []
                for i, calc in enumerate(variable.calculation.split(";")):
                    self.calculation.append(None)
                    if self.test_types[i] == "between":
                        if not isinstance(self.columns[i], list):
                            self.columns[i] = [self.columns[i]]
                        for c in self.columns[i]:
                            calc = calc.replace(c, 'float(row["' + c + '"])')
                        self.calculation[i] = compile(calc, "<string>", "eval")

            self.test_type = self.test_many
    
    def test(self, row):
        """
        Tests if current variable is true for row

        Args:
            row: a row from a form

        Returns:
            id(int): 0 if false and 1 (or sum) if true
        """
        return self.test_type(row)
    
    def test_many(self, row):

        res_dict = {}
        for i in range(len(self.test_types)):
            tt = self.test_types[i]
            if tt == "match":
                res = self.test_match(self.columns[i], self.conditions[i], row)
            elif tt == "sub_match":
                res = self.test_sub_match(self.columns[i], self.conditions[i],
                                          row)
            elif tt == "between":
                if not isinstance(self.columns[i], list):
                    self.columns[i] = [self.columns[i]]

                res = self.test_calc_between(self.columns[i],
                                             self.conditions[i],
                                             self.calculation[i], row)
            elif tt == "not_null":
                res = self.test_not_null(self.columns[i], row)

            else:
                res = self.test_functions[self.test_types[i]](row)
            res_dict[self.bool_variables[i]] = res
        return eval(self.bool_expression)
        
    def test_match(self, column, condition, row):
        """Test if value is in condition list"""
        try:
            return row[column] in condition
        except:
            return 0
        
    def test_sub_match(self, column, condition, row):
        """
        We first test if value is in the list, if not we check
        if value is a substring of any element in the list
        """

        add = 0
        try:
            if row[column] in condition:
                add = 1
            else:
                for c in condition:
                    if row[column] and c in row[column]:
                        add = 1
                        break
        except:
            pass
        return add

    def test_not_null(self, column, row):
        """ Value not equal None"""
        if column not in row:
            return 0
        value = row[column]
        return value is not "" and value is not None and value is not 0

    def test_value(self, row):
        """ Value not equal None"""
        if self.columns[0] not in row:
            return 0
        value = row[self.columns[0]]
        if value is not "" and value is not None and value is not 0:
            if self.calculation == "date":
                return parse(value).isoformat()
            else:
                return value
        else:
            return 0

    def test_calc_between(self,
                          columns,
                          condition,
                          calc,
                          row, ):
        """
        self. calc should be an expression with column names
        from the row and mathematical expression  understood by python.
        We then replace all column names with their numerical values
        and evalualte the resulting expression.

        """
        for c in columns:
            if c in row and row[c]:
                pass
            else:
                return 0
        result = float(eval(calc))
        return float(condition[0]) <= result and float(condition[1]) > result
          
    def test_calc(self, row):
        """
        self. calc should be an expression with column names from
        the row and mathematical expression understood by python.
        We then replace all column names with their numerical values
        and evalualte the resulting expression.

        """
        for c in self.columns[0]:
            if c in row and row[c]:
                pass
            else:
                row[c] = 0
        try:
            return float(eval(self.calculation))
        except ZeroDivisionError:
            return 0
