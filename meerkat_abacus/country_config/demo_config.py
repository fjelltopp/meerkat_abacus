""" Config for Demo Location """
import datetime
s3_bucket = False

country_config = {
    "country_name": "Demo",
    "tables": {
        "case": "demo_case",
        "alert": "demo_alert",
        "register": "demo_register",
    },
    "codes_file": "demo_codes",
    "links_file": "demo_links.py",
    "country_tests": "demo_test.py",
    "epi_week": "international",
    "locations": {
        "clinics": "demo_clinics.csv",
        "districts": "demo_districts.csv",
        "regions": "demo_regions.csv"
    },
    "form_dates": {
        "case": "pt./visit_date",
        "alert": "end",
        "register": "end",
    },
    "fake_data": {
        "case": {"pt1./age": {"integer": [0, 120]},
                 "pt1./gender": {"one": ["male", "female"]},
                 "pt./visit_date": {"date": "year"},
                 "intro./visit_type": {"one": ["new", "return", "referral"]},
                 "nationality": {"one": ["demo", "null_island"]},
                 "pt1./status": {"one": ["refugee", "national"]},
                 "intro_module": {"multiple": ["mh",
                                               "imci", "rh", "labs", "px"]},
                 "pregnant": {"one": ["yes", "no"]},
                 "pregnancy_complications": {"one": ["yes", "no"]},
                 "smoke_ever": {"one": ["yes", "no"]},
                 "smoke_now": {"one": ["yes", "no"]},
                 "icd_code": {"one": ["A80.9", "B05.3", "A00.1", "A00", "A39", "A87",
                                      "A03", "A36.8", "A33.3", "A34.4",
                                      "A35.4", "A37", "E15", "E16", "E20.4",
                                      "E40", "E41", "E50", "E65", "F40", "O60",
                                      "P70", "S10"]},
                 "vaccination_type": {"multiple": ["bcg", "hepb", "diptheria",
                                                   "tetanus", "pertussis",
                                                   "polio", "hib", "measles",
                                                   "mumps", "rubella"] },
                 "vaccination": {"one": ["yes", "no"]}},
        "register": {"consult./consultations": {"integer": [10, 20]},
                     "consult./consultations_refugee": {"integer": [5, 15]},
                     "surveillance./afp": {"integer": [1, 5]},
                     "surveillance./measles": {"integer": [1, 5]}
                     },
        "alert": {"pt./alert_id": {"data": "uuids"},
                  "alert_labs./return_lab": {"one": ["yes", "no", "unsure"]},
                  "pt./checklist": {"multiple": ["referral",
                                                 "case_management",
                                                 "contact_tracing",
                                                 "return_lab"]}},
        "other": []
    },
    "alert_data": {"age": "pt1./age", "gender": "pt1./gender"},
    "alert_id_length": 6,
    "messaging_start_date": datetime.datetime(2016, 2, 15),
    "messaging_topic_prefix": "null",
    "messaging_sender": "",
    "messaging_silent": False
}
