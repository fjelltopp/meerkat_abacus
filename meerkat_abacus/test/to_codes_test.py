import unittest

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from meerkat_abacus import model
from meerkat_abacus import config
from meerkat_abacus.codes.to_codes import to_code
from meerkat_abacus.codes.variable import Variable
from meerkat_abacus.data_management import set_up_everything, create_db,\
 add_fake_data,create_links,import_locations, import_variables,\
 import_data

# Data for the tests
locations = {1: model.Locations(name="Demo"),
             2: model.Locations(
                 name="Region 1", parent_location=1),
             3: model.Locations(
                 name="Region 2", parent_location=1),
             4: model.Locations(
                 name="District 1", parent_location=2),
             5: model.Locations(
                 name="District 2", parent_location=3),
             6: model.Locations(
                 name="Clinic 1", parent_location=4),
             7: model.Locations(
                 name="Clinic 2", parent_location=5),
             8: model.Locations(
                 name="Clinic with no district", parent_location=2)}
locations_by_deviceid = {1: 6, 2: 7, 3: 8}
regions = [2, 3]
districts = [4, 5]
data_type_definitions = [
  {
  "type":"case",
  "form":"demo_case",
  "db_column":"",
  "condition":"",
  "date":"pt./visit_date",
  "var":"tot_1",
  "uuid":"meta/instanceID"
  },
  {
  "type":"visit",
  "form":"demo_case",
  "db_column":"",
  "condition":"",
  "date":"pt./visit_date",
  "var":"vis_1",
  "uuid":"meta/instanceID"
  },
]

agg_variables = [
    model.AggregationVariables(
        id=1, method="not_null", db_column="index", condition="",
        category=[],
        form="form1"),
    model.AggregationVariables(
        id=2,
        method="match",
        db_column="column1",
        alert=1,
        category=[],
        alert_type="individual",
        condition="A",
        form="form1"),
    model.AggregationVariables(
        id=3,
        category=[],
        method="sub_match",
        db_column="column2",
        condition="B",
        form="form1"),
    model.AggregationVariables(
        id=4,
        category=[],
        method="between",
        calculation="column3",
        db_column="column3",
        condition="5,10",
        disregard=1,
        form="form1"),
    # test cases for priority logic
    model.AggregationVariables(
        id=5,
        category=[],
        method="match",
        calculation="column3",
        db_column="column3",
        condition="A"
        ),
]
alert_data = {"column1": "column1"}

devices = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: []}
all_locations = (locations, locations_by_deviceid, regions, districts, devices)

variables = {"case": {1: {}, 2: {}, 3: {}, 4: {}, 5: {}}}
variables_forms = {}
variables_test = {}
variables_groups = {}
mul_forms = []

for av in agg_variables:
    variables["case"][av.id][av.id] = Variable(av)
    variables_forms[av.id] = "form1"
    variables_test[av.id] = variables["case"][av.id][av.id].test_type
    variables_groups[av.id] = [av.id]


class ToCodeTest(unittest.TestCase):
    """
    Test the to_code functionality
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_location_information(self):
        """
        Testing that all the location infomation is translated correctly
        """
        row = {"form1":
               {"index": 1,
                "column1": "A",
                "column2": "B34",
                "column3": "7",
                "date": "2015-10-25",
                "deviceid": 1,
                "meta/instanceID": "a"}}
        var, category, ret_location, disregarded = to_code(
            row,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(ret_location["country"], 1)
        self.assertEqual(ret_location["region"], 2)
        self.assertEqual(ret_location["district"], 4)
        self.assertEqual(ret_location["clinic"], 6)

        row["form1"]["deviceid"] = 2
        var, category, ret_location, disregard = to_code(
            row,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(ret_location["country"], 1)
        self.assertEqual(ret_location["region"], 3)
        self.assertEqual(ret_location["district"], 5)

        row["form1"]["deviceid"] = 3
        var, category, ret_location, disregard = to_code(
            row,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(ret_location["country"], 1)
        self.assertEqual(ret_location["region"], 2)
        self.assertEqual(ret_location["district"], None)
        row["form1"]["deviceid"] = 99
        var, category, ret_location, disregard = to_code(
            row,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(ret_location, None)

    def test_links(self):
        """
        Checking that links are generated correctly
        """
        
        create_db(config.DATABASE_URL, model.Base, drop=True)
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        conn = engine.connect()
        import_locations(engine, session)
        import_variables(session)
        add_fake_data(session, N=0, append=False, from_files=True)
        import_data(engine=engine,session=session)
        session.commit()


        query =  session.query(model.form_tables["demo_case"])

        res = conn.execution_options(
            stream_results=False).execute(query.statement)

        for data_type in data_type_definitions:
        #data_type=data_type_definitions[0]
          create_links(data_type=data_type, input_conditions=[], table=model.form_tables[data_type["form"]], session=session, conn=conn)
        #data_type=data_type_definitions[0]
        #create_links(data_type=data_type, input_conditions=[], table=model.form_tables[data_type["form"]], session=session, conn=conn)


        # use predetermined test cases to check link generation#
        test_cases=[
        ["uuid:init_visit_p70","uuid:return_visit_p70","return_visit"],
        ["uuid:init_visit_e65","uuid:return_visit_e65","return_visit"],
        ["uuid:false_init_visit_e65","uuid:return_visit_e65","return_visit"],
        ["uuid:return_visit_p70","uuid:init_visit_p70","initial_visit"]
        ]

        for test_case in test_cases:

          link_query_condition = and_(model.Links.uuid_from == test_case[0], model.Links.type == test_case[2])
          query =  session.query(model.Links).filter(link_query_condition)

          res = conn.execute(query.statement).fetchall()
          # make sure that the item the link links to is the one defined above
          self.assertEqual(len(res),1)
          self.assertEqual(res[0]["uuid_to"],test_case[1])


    def test_priority(self):
        

        pass



    def test_variables(self):
        """
        Checking that variables returned and alerts are working
        """
        row1 = {"form1": {"index": 1,
                          "column1": "A",
                          "column2": "B34",
                          "column3": "7",
                          "date": "2015-10-25",
                          "deviceid": 1,
                          "meta/instanceID": "a"}}
        row2 = {"form1": {"index": 2,
                          "column1": "B",
                          "column2": "A",
                          "column3": "4",
                          "date": "2015-10-25",
                          "deviceid": 2,
                          "meta/instanceID": "b"}}
        row3 = {"form1": {"index": 1,
                          "column1": "A",
                          "column2": "C",
                          "column3": "7",
                          "date": "2015-10-25",
                          "deviceid": 2,
                          "meta/instanceID": "c"}}
        var, category, ret_loc, disregard = to_code(
            row1,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(var, {1: 1,
                               2: 1,
                               3: 1,
                               4: 1,
                               'alert_reason': 2,
                               'alert': 1,
                               'alert_type': "individual",
                               'alert_column1': 'A'})
        self.assertEqual(disregard, True)
        var, category, ret_loc, disregard = to_code(
            row2,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(var, {1: 1})
        self.assertEqual(disregard, False)
        var, category, ret_loc, disregard = to_code(
            row3,
            (variables, variables_forms, variables_test, variables_groups),
            all_locations, "case", "form1", alert_data, mul_forms)
        self.assertEqual(var, {1: 1,
                               2: 1,
                               4: 1,
                               'alert': 1,
                               'alert_column1': 'A',
                               "alert_type": "individual",
                               'alert_reason': 2})
        self.assertEqual(disregard, True)

if __name__ == "__main__":
    unittest.main()
