"""
Database model definition
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, DDL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import validates
from sqlalchemy.event import listen

from meerkat_abacus.config import country_config

Base = declarative_base()


form_tables = {}

for table in country_config["tables"]:
    form_tables[table] = type(table, (Base, ),
                              {"__tablename__": table,
                               "id": Column(Integer, primary_key=True),
                               "uuid": Column(String),
                               "data": Column(JSONB)})


class Locations(Base):
    __tablename__ = 'locations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    parent_location = Column(Integer, index=True)
    geolocation = Column(String)
    other = Column(String)
    deviceid = Column(String)
    clinic_type = Column(String)
    case_report = Column(Integer, index=True)
    level = Column(String, index=True)
    start_date = Column(DateTime)

    def __repr__(self):
        return "<Location(name='%s', id='%s', parent_location='%s')>" % (
            self.name, self.id, self.parent_location)

class Data(Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)
    uuid = Column(String)
    type = Column(String, index=True)
    date = Column(DateTime, index=True)
    country = Column(Integer, index=True)
    region = Column(Integer, index=True)
    district = Column(Integer, index=True)
    clinic = Column(Integer, index=True)
    clinic_type = Column(String)
    variables = Column(JSONB, index=True)
    geolocation = Column(String)
    
    def __repr__(self):
        return "<Data(uuid='%s', id='%s'>" % (
            self.uuid, self.id )

create_index = DDL("CREATE INDEX variables_gin ON data USING gin(variables);")
listen(Data.__table__, 'after_create', create_index)


class AggregationVariables(Base):
    __tablename__ = 'aggregation_variables'

    id = Column(String, primary_key=True)
    name = Column(String)
    type = Column(String)
    form = Column(String)
    db_column = Column(String)
    method = Column(String)
    condition = Column(String)
    category = Column(JSONB)
    alert = Column(Integer)
    calculation = Column(String)
    calculation_group = Column(String)
    classification = Column(String)
    classification_casedef = Column(String)
    source = Column(String)
    source_link = Column(String)
    alert_desc = Column(String)
    case_def = Column(String)
    risk_factors = Column(String)
    symptoms = Column(String)
    labs_diagnostics = Column(String)
    
    def __repr__(self):
        return "<AggregationVariable(name='%s', id='%s'>" % (
            self.name, self.id)


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
