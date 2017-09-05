import unittest

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists
import os
from meerkat_abacus import model, config, util, tasks
from meerkat_abacus.test import util as test_util
from meerkat_abacus import data_management

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

        tasks.set_up_db()
        self.engine, self.session = util.get_db_engine()
        current_directory = os.path.dirname(os.path.realpath(__file__))
        test_util.add_data_from_file(
            current_directory + "/test_data/test_cases/" + "demo_case_link_test_data.csv",
            "demo_case")
        test_util.add_data_from_file(
            current_directory + "/test_data/test_cases/" + "demo_case_exclusion_list_test_data.csv",
            "demo_case")
        test_util.add_data_from_file(
            current_directory + "/test_data/test_cases/" + "demo_case_duplicate_initial_visits_test_data.csv",
            "demo_case")
        data_management.initial_visit_control()
        data_management.new_data_to_codes()
        self.session.commit()

    def tearDown(self):
        self.session.commit()
        self.session.close()
        self.engine.dispose()


    #nosetests -v --nocapture meerkat_abacus.test.link_test:LinkTest.test_exclusion_lists`…`nosetests -v --nocapture`
    def test_exclusion_lists(self):
        """
        Check that the uuids in the exclusion lists are actually removed
        """
        for form in config.country_config.get("exclusion_lists", []):
            print(str(form))
            for exclusion_list_file in config.country_config["exclusion_lists"][form]:
                exclusion_list = util.read_csv(config.config_directory + exclusion_list_file)
                for uuid_to_be_removed in exclusion_list:
                    query = self.session.query(model.form_tables[form]).\
                            filter(model.form_tables[form].uuid == uuid_to_be_removed["uuid"])
                    res = query.all()
                    self.assertEqual(len(res), 0)

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

          res = query.all()
          print("DEBUG: " + str(res) + "\n")

          # make sure that the item the link links to is the one defined above
          self.assertEqual(len(res), 1)
          self.assertEqual(res[0].uuid_to, test_case[1])

    def test_priority(self):
        """
        Checking that the variable priority logic functions correctly
        """

        self.assertTrue(database_exists(config.DATABASE_URL))

        # use predetermined test cases to check link generation#
        test_cases = [
          ["uuid:init_visit_p70", "gen_2"],
          ["uuid:init_visit_e65", "gen_2"],
          ["uuid:false_init_visit_e65", "gen_2"]
        ]

        for test_case in test_cases:
            query = self.session.query(model.Data).filter(
                and_(model.Data.uuid == test_case[0],
                     model.Data.type == 'case'))
            res = query.all()
            
            print("DEBUG: " + str(res) + "\n")
            
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0].variables[test_case[1]], 1)
