""" Config for Demo Location """
import datetime
s3_bucket = False

country_config = {
    "country_name": "Demo",
    "tables": [
        "demo_case",
        "demo_alert",
        "demo_register",
#        "plague_data"
    ],
    "initial_visit_control": {
        "demo_case": {
          "identifier_key_list": ["pt./pid","icd_code"],
          "visit_type_key": "intro./visit",
          "visit_date_key": "pt./visit_date",
          "module_key": "intro./module",
          "module_value": "ncd"
        }

    },
    "require_case_report": ["demo_case", "demo_register"],
    "codes_file": "demo_codes",
    "coding_list": [
        "demo_codes.csv",
        "demographic_codes.csv",
        "icd_codes.csv"
    ],
    "links_file": "demo_links.csv",
    "types_file": "data_types.csv",
    "country_tests": "demo_test.py",
    "epi_week": "day:0",
    "locations": {
        "clinics": "demo_clinics.csv",
        "districts": "demo_districts.csv",
        "regions": "demo_regions.csv",
        "zones": "demo_zones.csv"
    },
    "geojson_files": ["demo_json.json"],
    "calculation_parameters": ["vaccination_vials.json","medicine_kits.json"],
    "exclusion_list":"demo_exclusion.csv",
    "form_dates": {
        "case": "pt./visit_date",
        "alert": "end",
        "register": "end",
    },
    "fake_data": {
        "demo_case": {
            "pt1./age": {"integer": [0, 120]},
            "child_age": {"integer": [0, 60]},
            "pt./pid": {"patient_id": "intro./visit;new"},
            "pt1./gender": {"one": ["male", "female"]},
            "pt./visit_date": {"date": "year"},
            "intro./visit": {"one": ["new", "return", "referral"]},
            "nationality": {"one": ["demo", "null_island"]},
            "pt1./status": {"one": ["refugee", "national"]},
            "intro_module": {
                "multiple": ["mh","imci", "rh", "labs", "px"]
            },
            "symptoms": {"multiple": ["A9_4", "B15-17_2", "A39_3",
                         "A03_2", "!0_1", "", ""]},
            "pregnant": {"one": ["yes", "no"]},
            "pregnancy_complications": {"one": ["yes", "no"]},
            "smoke_ever": {"one": ["yes", "no"]},
            "smoke_now": {"one": ["yes", "no"]},
            "icd_code": {"one": ["A80.9", "B05.3", "A00.1", "A00", "A39", "A87",
                               "A03", "A36.8", "A33.3", "A34.4","E10", "G08", "J40",
                               "A35.4", "A37", "E15", "E16", "E20.4","I10",
                               "E40", "E41", "E50", "E65", "F40", "O60",
                               "P70", "S10"]},
            "vaccination_type": {"multiple": ["bcg", "hepb", "diptheria",
                                            "tetanus", "pertussis",
                                            "polio", "hib", "measles",
                                            "mumps", "rubella"]},
            "pip./namru": {"one": range(1, 400)},
            "patientid": {"one": range(1, 20)},
            "sari": {"one": ["yes", "no"]},
            "pip_fu5./icu": {"one": ["yes", "no"]},
            "pip_fu5./ventilated": {"one": ["yes", "no"]},
            "pip_fu7./outcome": {"one": ["death", "not"]},
            "results./bp_systolic": {"integer": [0, 200]},
            "results./bp_diastolic": {"integer": [0, 100]},
            "results./bmi_weight": {"integer": [40, 120]},
            "results./bmi_height": {"integer": [120, 210]},
            "results./glucose_fasting": {"integer": [1, 200]},
            "results./hba1c": {"integer": [0, 20]},
            "vaccination": {"one": ["yes", "no"]},
            "risk_code": {"multiple-spaces":  ["A80_5", "D67-67-2",
                                      "E10-14_9"]},
            "sympt_code": {"multiple-spaces": ["A80_3", "B05_8", ""]},
            "breastfeed": {"one": ["yes", "no"]},
            "exclusive_breastfeed": {"one": ["yes", "no"]},
            "formula": {"one": ["yes", "no"]}},
        "demo_register": {
            "intro./module": {"one": ["ncd", "cd"]},
            "consult./consultations": {"integer": [10, 20]},
            "consult./ncd_consultations": {"integer": [10, 20]},
            "consult./consultations_refugee": {"integer": [5, 15]},
            "surveillance./afp": {"integer": [1, 5]},
            "surveillance./measles": {"integer": [1, 5]}
        },
        "demo_alert": {"pt./alert_id": {"data": "uuids"},
                       "alert_labs./return_lab": {"one": ["yes", "no", "unsure"]},
                       "pt./checklist": {"multiple": ["referral",
                                                      "case_management",
                                                      "contact_tracing",
                                                      "return_lab"]}},
        "demo_labs": {
            "labs./namru": {"one": range(1, 400)},
            "pcr./flu_a_pcr": {"one": ["positive", "negative"]},
            "pcr./h1_pcr": {"one": ["positive", "negative"]},
            "pcr./h3_pcr": {"one": ["positive", "negative"]},
            "pcr./h1n1_pcr": {"one": ["positive", "negative"]},
            "pcr./sw_n1_pcr": {"one": ["positive", "negative"]},
            "pcr./flu_b_pcr": {"one": ["positive", "negative"]}
        },
        "plague_data": {
            "pt./visit_date": {"date": "year"},
            "lat": {"range": [0, 0.4]},
            "lng": {"range": [0, 0.4]},
            "pt1./gender1": {"one": ["male", "female"]},
            "pt2./gender2": {"one": ["male", "female"]}
        }
    },
    "manual_test_data": {
      "demo_case":[
        "demo_case_link_test_data",
        "demo_case_duplicate_initial_visits_test_data",
        "demo_case_exclusion_list_test_data"
        ]
    },
    "exclusion_lists": {
      "demo_case":[
        "demo_case_exclusion_list.csv"
      ]
    },
    "alert_data": {"demo_case": {"age": "pt1./age", "gender": "pt1./gender"}},
    "alert_id_length": 6,
    "alert_text_content": ['reason', 'date', 'clinic', 'region', 'patient', 'gender', 'age', 'id', 'submitted', 'received', 'id' ],
    "alert_sms_content": ['reason', 'date', 'clinic', 'region', 'gender', 'age', 'id'],
    "alert_email_content": [
        'reason', 'date', 'clinic', 'region', 'breaker', 'patient', 'gender', 'age', 'breaker', 'submitted', 'received', 'id'
    ],
    "messaging_start_date": datetime.datetime(2016, 2, 15),
    "messaging_topic_prefix": "null",
    "messaging_sender": "",
    "messaging_silent": True,
    "default_start_date": datetime.datetime(2016, 1, 1),
    "reports_schedule": {
        "cd_public_health": {"period": "week", "send_day": "0", "language": "en"},
        "ncd_public_health": {"period": "month", "send_day": "1", "language": "en"},
        "communicable_diseases": {"period": "week", "send_day": "0", "language": "en"},
        "non_communicable_diseases": {"period": "month", "send_day": "1", "language": "en"}
    },
    "device_message_schedule": {
            "thank_you": {
                "period": "week",
                "send_day": "0",
                "message": "Thank you for all your work this week in sending data and please keep up the good work!",
                "distribution": ["/topics/demo"]}
    },
    "translation_dir": "/var/www/meerkat_frontend/country_config/translations"
}

dhis2_config = {
    "url": "http://localhost:8085",
    "apiResource": "/api/26/",
    "credentials": ('admin', 'district'),
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46ZGlzdHJpY3Q="
    },
    "loggingLevel": "DEBUG",
    # "countryId": "EebWN0q7GpT", # Null Island
    "countryId": "ImspTQPwCqd", # Sierra Leone
    "forms": [
        {
            "name": "demo_case",
            "event_date": "pt./visit_date",
            "completed_date": "end",
            # "programId": "fgrH0jPDNEP", # optional
            "status": "COMPLETED"
        },
        {
            "name": "demo_alert",
            "date": "end"
        },
        {
            "name": "demo_register",
            "date": "intro./visit_date"
        }
    ]
}
