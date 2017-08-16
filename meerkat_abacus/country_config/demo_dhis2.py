dhis2_config = {
    "url": "http://54.76.53.0:8080",
    "api_resource": "/api/26/",
    "credentials": ('admin', 'district'),
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46ZGlzdHJpY3Q="
    },
    # "country_id": "k83FUJTHUel",
    "country_id": "EebWN0q7GpT", # Null Island
    "forms": [
        {
            "name": "demo_case",
            "event_date": "pt./visit_date",
            "completed_date": "end",
            # "program_id": "fgrH0jPDNEP",
            "stored_by": "admin",
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
