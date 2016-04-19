"""
Testing for DB utilities
"""

import unittest
from meerkat_abacus.data_management import prepare_link_data

class LinkTest(unittest.TestCase):

    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    def test_prepare_link_data(self):
        data_def = {
            "Status": {
                "Ongoing": {"column": "alert_labs./return_lab",
                            "condition": ["", "unsure"]},
                "Confirmed": {"column": "alert_labs./return_lab",
                              "condition": "yes"},
                "Discarded": {"column": "alert_labs./return_lab",
                              "condition": "no"}
            },
            "checklist": {
                "Referral": {"column": "pt./checklist",
                             "condition": "referral"},
                "Case Managment": {"column": "pt./checklist",
                                   "condition": "case_management"},
                "Contact Tracing": {"column": "pt./checklist",
                                    "condition": "contact_tracing"},
                "Laboratory Diagnosis": {"column": "pt./checklist",
                                         "condition": "return_lab"},
            }
        }
        row1 = {"alert_labs./return_lab": "yes",
                "pt./checklist": "referral,case_management"}
        new_data = prepare_link_data(data_def, row1)
        assert("Status" in new_data.keys())
        assert("checklist" in new_data.keys())
        assert(new_data["Status"] == "Confirmed")
        assert("Referral" in new_data["checklist"])
        assert("Case Managment" in new_data["checklist"])
        row2 = {"alert_labs./return_lab": "unsure",
                "pt./checklist": "referral,case_management"}
        new_data = prepare_link_data(data_def, row2)
        assert(new_data["Status"] == "Ongoing")
        row2 = {"alert_labs./return_lab": "",
                "pt./checklist": "referral,case_management"}
        new_data = prepare_link_data(data_def, row2)
        assert(new_data["Status"] == "Ongoing")
        
if __name__ == "__main__":
    unittest.main()
