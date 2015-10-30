"""
Configuration for meerkat_abacus
"""
DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@db/meerkat_db'

form_directory = "../data/forms/"

country_config = {
    "country_name": "Demo",
    "tables": {
        "case": "demo_case",
        "alert": "demo_alert",
        "register": "demo_register",
    },
    "codes_file": "demo_codes",
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
                 "nationality": {"one": ["demo", "null-island"]},
                 "pt1./status": {"one": ["refugee", "national"]},
                 "intro_module": {"multiple": ["mh",
                                               "imci", "rh", "labs", "px"]},
                 "pregnant": {"one": ["yes", "no"]},
                 "pregnancy_complications": {"one": ["yes", "no"]},
                 "icd_code": {"one": ["A80.9", "B05.3", "A00.1", "A39", "A87",
                                      "A03", "A36.8", "A33.3", "A34.4",
                                      "A35.4", "A37", "E15", "E16", "E20.4",
                                      "E40", "E41", "E50", "E65", "F40", "O60",
                                      "P70", "S10"]},
                 "vaccination_type": {"multiple": ["bcg", "hepb", "diptheria",
                                                   "tetanus", "pertussis",
                                                   "polio", "hib", "measles",
                                                   "mumps", "rubella"] }},
        "register": {"consult./consultations": {"integer": [10, 20]},
                     "consult./consultations_refugee": {"integer": [5, 15]}},
        "alert": {"pt./alert_id": {"data": "uuids"},
                  "alert_labs./return_lab": {"one": ["yes", "no", "unsure"]}},
        "other": []
    },
    "alert_id_length": 6,
}
