import unittest

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists

from meerkat_abacus import model, config, util
from meerkat_abacus.codes.to_codes import to_code
from meerkat_abacus.codes.variable import Variable
from meerkat_abacus.data_management import set_up_everything, create_db,\
 add_fake_data,create_links,import_locations, import_variables,\
 import_data, new_data_to_codes

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

class LinkTest(unittest.TestCase):
    """
	Test links functionality
    """

    def setUp(self):
    	# Link processing is done in the database, so the database needs to be set up
        create_db(config.DATABASE_URL, drop=True)
        engine = create_engine(config.DATABASE_URL)
        model.Base.metadata.create_all(engine)
        self.engine = create_engine(config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.conn = self.engine.connect()
        import_locations(self.engine, self.session)
        import_variables(self.session)
        add_fake_data(self.session, N=0, append=False, from_files=True)
        import_data(engine=self.engine,session=self.session)

        for data_type in data_type_definitions:
          create_links(data_type=data_type, input_conditions=[],
           table=model.form_tables[data_type["form"]], session=self.session, conn=self.conn)
          self.session.commit()

        new_data_to_codes(self.engine)

        self.session.commit()

    def tearDown(self):
        self.session.commit()
        self.conn.close()
        self.session.close()
        self.engine.dispose()


    #nosetests -v --nocapture meerkat_abacus.test.link_test:LinkTest.test_exclusion_lists`…`nosetests -v --nocapture`
    def test_exclusion_lists(self):
        """
        Check that the uuids in the exclusion lists are actually removed
        """
        for form in config.country_config.get("exclusion_lists",[]):
          print(str(form))
          for exclusion_list_file in config.country_config["exclusion_lists"][form]:
            exclusion_list = util.read_csv(config.config_directory + exclusion_list_file)
            for uuid_to_be_removed in exclusion_list:
                query = self.session.query(model.form_tables[form]).\
                    filter(model.form_tables[form].uuid == uuid_to_be_removed["uuid"])
                res = self.conn.execute(query.statement).fetchall()
                self.assertEqual(len(res),0)

    def test_links(self):
        """
        Checking that links are generated correctly
        """
        self.assertTrue(database_exists(config.DATABASE_URL))

        # use predetermined test cases to check link generation#
        test_cases=[
          ["uuid:init_visit_p70","uuid:return_visit_p70","return_visit"],
          ["uuid:init_visit_e65","uuid:return_visit_e65","return_visit"],
          ["uuid:false_init_visit_e65","uuid:return_visit_e65","return_visit"],
          ["uuid:return_visit_p70","uuid:init_visit_p70","initial_visit"]
        ]

        for test_case in test_cases:

          link_query_condition = and_(model.Links.uuid_from == test_case[0], model.Links.type == test_case[2])
          query =  self.session.query(model.Links).filter(link_query_condition)

          res = self.conn.execute(query.statement).fetchall()
          print("DEBUG: " + str(res) + "\n")

          # make sure that the item the link links to is the one defined above
          self.assertEqual(len(res),1)
          self.assertEqual(res[0]["uuid_to"],test_case[1])

    def test_priority(self):
        """
        Checking that the variable priority logic functions correctly
        """

        self.assertTrue(database_exists(config.DATABASE_URL))

        # use predetermined test cases to check link generation#
        test_cases=[
          ["uuid:init_visit_p70","gen_2"],
          ["uuid:init_visit_e65","gen_2"],
          ["uuid:false_init_visit_e65","gen_2"]
        ]

        for test_case in test_cases:
          query =  self.session.query(model.Data).filter(and_(model.Data.uuid==test_case[0], model.Data.type=='case'))

          res = self.conn.execution_options(
              stream_results=False).execute(query.statement).fetchall()

          print("DEBUG: " + str(res) + "\n")

          self.assertEqual(len(res),1)
          self.assertEqual(res[0]["variables"][test_case[1]],1)
