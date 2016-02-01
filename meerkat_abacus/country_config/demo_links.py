
links = [
    {
        "id": "alert_investigation",
        "name": "Alert Investigation",
        "from_table": "Alerts",
        "from_column": "id",
        "from_date": "date",
        "to_table": "alert",
        "to_column": "pt./alert_id",
        "to_date": "end",
        "which": "last",
        "data": {
            "status": {
                "Ongoing": {"column": "alert_labs./return_lab",
                            "condition": ["", "unsure"]},
                "Confirmed": {"column": "alert_labs./return_lab",
                              "condition": "yes"},
                "Disregarded": {"column": "alert_labs./return_lab",
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
            },
            "investigator": {
                "investigator": {"column": "deviceid",
                                 "condition": "get_value"
                                 }
                }
        }

    }
]

                                     
                                           
                            
