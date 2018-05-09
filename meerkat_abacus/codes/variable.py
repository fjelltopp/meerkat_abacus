"""
Definition of the Variable class


"""
from dateutil.parser import parse
from functools import partial
from datetime import datetime, timedelta
from meerkat_abacus.config import config
country_config = config.country_config


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
                    raise NameError(
                        "{} has wrong test type".format(variable.id)
                    )
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
                if '' in c:
                    c.append(None)
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
                    c, 'row["' + c + '"]')
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
                        c, 'row["' + c + '"]'
                    )
                self.calculation = compile(
                    self.calculation, "<string>", "eval"
                )
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
                            calc = calc.replace(
                                c, 'row["' + c + '"]')
                        calc = compile(calc, "<string>", "eval")
                        self.calculation[i] = calc

            self.test_type = self.test_many

        if hasattr(variable, "calculation_priority"):
            self.calculation_priority = variable.calculation_priority

    def test(self, row):
        """
        Tests if current variable is true for row

        Args:
            row: a row from a form

        Returns:
            {
            applicable: 1 or 0,
            value: return value
            }
        """
        applicable = self.test_type(row)
        value = applicable
        if self.test_types[0] == "calc" and applicable == 0:
            applicable = 1
        return {"applicable": applicable,
                "value": value}

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
                if value:
                    try:
                        return parse(value).isoformat()
                    except ValueError:
                        print(value)
                        return 0
            else:
                return value
        else:
            return 0

    def test_calc_between(self,
                          columns,
                          condition,
                          calc,
                          old_row):
        """
        self. calc should be an expression with column names
        from the row and mathematical expression  understood by python.
        We then replace all column names with their numerical values
        and evalualte the resulting expression.

        """

        row = {}
        for c in columns:
            # Initialise non-existing variables to 0.
            if c not in old_row or old_row[c] == '' or old_row[c] is None:
                return 0

            # If row[c] is a datestring convert to #seconds.
            try:
                row[c] = float(old_row[c])
            except ValueError:
                row[c] = old_row[c]

        try:
            result = float(eval(calc))
            greater = float(condition[0]) <= result
            less = float(condition[1]) > result
            return greater & less
        except ZeroDivisionError:
            return 0
        except ValueError as e:
            print("Value error while testing for code ", self.variable.id)
            raise e
            

    def test_calc(self, old_row):
        """
        self. calc should be an expression with column names from
        the row and mathematical expression understood by python.
        We then replace all column names with their numerical values
        and evalualte the resulting expression.

        If the column value is a date, we replace with the number of
        seconds since epi week start after epoch (e.g the first
        sunday after epoch for Jordan).

        """
        row = {}
        for c in self.columns[0]:

            # Initialise non-existing variables to 0.
            if c not in old_row:
                return 0

            if old_row[c] == '' or old_row[c] is None:
                row[c] = 0
            else:
                try:
                    row[c] = float(old_row[c])
                except ValueError:
                    row[c] = old_row[c]

        try:
            return float(eval(self.calculation))
        
        except ZeroDivisionError:
            return 0

    @staticmethod
    def to_date(element):
        """
        Converts a row element date string to a number, if the element conforms
        to one of the specified date formats. If the specified row element is a
        datestring, this function calulates the number of seconds between that
        datetime and the epi week start after the epoch i.e. in Jordan, the
        first Sunday after 1st January 1970. If the specified row element
        doesn't conform to an acceptable date string form, it just returns the
        element instead.
        """
        # If element isn't even a string, just return the element instantly.
        if type(element) is not str:
            return element


        # For each format, try to parse and convert a date from the element.
        # If parsing fails, try the next format.
        # If success, return the converted date.
        for i, date_format in enumerate(allowed_formats):

            try:
                date = parse_date(element, date_format)
                # Want to calc using secs from the epi week start after epoch.
                # Let's call this the epiepoch. Epoch was on Thurs 1/1/1970, so
                # (4 + epi_week_start_day) % 7 = days between epoch & epiepoch
                if isinstance(country_config['epi_week'], str):
                    epi_offset = (4 + int(country_config['epi_week'][4:])) % 7
                else:
                    year = date.year
                    epi_offset = (
                        4 + country_config["epi_week"].get(year, datetime(year, 1, 1)).weekday()
                    ) % 7
                # Time since epiepoch = date - epiepoch
                # Where epiepoch = epoch + epioffset.
                since_epi_epoch = date - (datetime(1970, 1, 1) +
                                          timedelta(days=epi_offset))

                # Return the calculated number of seconds.
                return since_epi_epoch.total_seconds()

            # If failed to parse date, try a different acceptable date format.
            except (ValueError, KeyError):
                pass

        # If the element didn't conform to a date format, just return element.
        return element

# A list of the valid datestring formats
allowed_formats = [
    '%b %d, %Y',
    '%d-%b-%Y',
    '%Y-%m-%d',
    '%d-%b-%Y %I:%M:%S',
    '%d-%b-%Y %H:%M:%S',
    '%b %d, %Y %I:%M:%S %p',
    '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S.%fZ',
    '%Y-%m-%dT%H:%M:%S'
]

    
months = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12
}


def parse_date(string, date_format):
    if date_format == '%b %d, %Y':
        year = string[-4:]
        f = string[:-6]
        mon = f[0:3]
        day = f[3:]
        return datetime(int(year), months[mon], int(day))
    else:
        return datetime.strptime(string, date_format)
