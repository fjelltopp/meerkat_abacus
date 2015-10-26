"""
Model
"""

from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


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
    dbtable = Column(String)
    dbcolumn = Column(String)
    method = Column(String)
    location = Column(String)
    condtion = Column(String)
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

