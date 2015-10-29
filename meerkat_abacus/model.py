"""
Database model definition
"""
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import validates

from meerkat_abacus.config import country_config

Base = declarative_base()

form_tables = {"case": None, "register": None, "alert": None, "other": {}}

for table in country_config["tables"]:
    table_name = country_config["tables"][table]
    if table != "other":
        form_tables[table] = type(table_name, (Base, ),
                                  {"__tablename__": table_name,
                                   "id": Column(Integer, primary_key=True),
                                   "uuid": Column(String),
                                   "data": Column(JSONB)})
    else:
        table_names = country_config["tables"][table]
    
        for table in table_names:
            form_tables["other"][table] = (
                type(table, (Base, ),
                     {"__tablename__": table,
                      "id": Column(Integer, primary_key=True),
                      "uuid": Column(String),
                      "data": Column(JSONB)}))

class Locations(Base):
    __tablename__ = 'locations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    parent_location = Column(Integer)
    geolocation = Column(String)
    other = Column(String)
    deviceid = Column(String)
    clinic_type = Column(String)
    case_report = Column(Integer)

    def __repr__(self):
        return "<Location(name='%s', id='%s', parent_location='%s')>" % (
            self.name, self.id, self.parent_location)

class AggregationVariables(Base):
    __tablename__ = 'aggregation_variables'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    form = Column(String)
    db_column = Column(String)
    method = Column(String)
    location = Column(String)
    condition = Column(String)
    category = Column(String)
    daily = Column(Integer)
    classification = Column(String)
    alert = Column(Integer)
    secondary_condition = Column(String)
    classification_casedef = Column(String)
    source = Column(String)
    source_link = Column(String)
    alert_desc = Column(String)
    case_def = Column(String)
    risk_factors = Column(String)
    symptoms = Column(String)
    labs_diagnostics = Column(String)

    @validates("alert")
    def alert_setter(self, key, alert):
        if alert == "":
            return 0
        else:
            return alert

    @validates("daily")
    def daily_setter(self, key, daily):
        if daily == "":
            return 0
        else:
            return daily

class Aggregation(Base):
    __tablename__ = 'aggregation'

    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime)
    interval = Column(Integer)
    variable = Column(Integer)
    location = Column(Integer)
    value = Column(Float)
    classification = Column(String)
    geo_location = Column(String)

class Alerts(Base):
    __tablename__ = 'alerts'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    reason = Column(String)
    location = Column(Integer)
    uuids = Column(String)

class AlertNotifications(Base):
    __tablename__ = 'alert_notifications'

    id = Column(Integer, primary_key=True)
    alert_id = Column(String)
    date = Column(DateTime)
    receivers = Column(String)

