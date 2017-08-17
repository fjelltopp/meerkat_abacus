dhis2_config = {
    "url": "http://54.76.53.0:8080",
    "apiResource": "/api/26/",
    "credentials": ('admin', 'district'),
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46ZGlzdHJpY3Q="
    },
    "loggingLevel": "DEBUG",
    # "country_id": "k83FUJTHUel",
    "countryId": "EebWN0q7GpT", # Null Island
    "forms": [
        {
            "name": "demo_case",
            "event_date": "pt./visit_date",
            "completed_date": "end",
            # "programId": "fgrH0jPDNEP",
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
