"""
Configuration for meerkat_abacus
"""
import os

DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@db/meerkat_db'
new_db_config = os.environ.get("MEERKAT_ABACUS_DB_URL")
if new_db_config:
    DATABASE_URL = new_db_config

data_directory = "~/meerkat_abacus/data/"
env_data_directory = os.environ.get("DATA_DIRECTORY")
if env_data_directory:
    data_directory = env_data_directory


fake_data = True
add_fake_data = os.environ.get("NEW_FAKE_DATA")
if add_fake_data:
    fake_data = add_fake_data

start_celery = False
env_start_celery = os.environ.get("START_CELERY")
if env_start_celery:
    start_celery = env_start_celery
interval = 3600  # Seconds

country_config = {
    "country_name": "Demo",
    "tables": {
        "case": "demo_case",
        "alert": "demo_alert",
        "register": "demo_register",
    },
    "codes_file": "demo_codes",
    "links_file": "demo_links",
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
                  "alert_labs./return_lab": {"one": ["yes", "no", "unsure"]},
                  "pt./checklist": {"multiple": ["referral",
                                                 "case_management",
                                                 "contact_tracing",
                                                 "return_lab"]}},
        "other": []
    },
    "alert_data": {"age": "pt1./age", "gender": "pt1./gender"},
    "alert_id_length": 6,
}
