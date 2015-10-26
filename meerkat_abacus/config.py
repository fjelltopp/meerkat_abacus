"""
Configuration for meerkat_abacus
"""
DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@db/meerkat_db'

country_config = {
    "country_name": "Demo",
    "tables": {
        "case": "demo_case",
        "alert": "demo_alert",
        "register": "demo_register",
        "other": []
    },
    "locations": {
        "clinics": "demo_clinics.csv",
        "districts": "demo_districts.csv",
        "regions": "demo_regions.csv"
    }
}
