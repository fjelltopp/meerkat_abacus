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
