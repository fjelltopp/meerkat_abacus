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


form_tables = {"case": None, "register": None, "alert": None}

for table in country_config["tables"]:
    table_name = country_config["tables"][table]
    form_tables[table] = type(table_name, (Base, ),
                            {"__tablename__": table_name,
                             "id": Column(Integer, primary_key=True),
                             "uuid": Column(String),
                             "data": Column(JSONB)})


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
    level = Column(String)

    def __repr__(self):
        return "<Location(name='%s', id='%s', parent_location='%s')>" % (
            self.name, self.id, self.parent_location)

class Data(Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)
    uuid = Column(String)
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
    form = Column(String)
    db_column = Column(String)
    method = Column(String)
    condition = Column(String)
    category = Column(JSONB)
    daily = Column(Integer)
    classification = Column(String)
    alert = Column(Integer)
    calculation_group = Column(String)
    secondary_condition = Column(String)
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

class LinkDefinitions(Base):
    __tablename__ = 'link_definitions'

    id = Column(String, primary_key=True)
    name = Column(String)
    from_table = Column(String, index=True)
    from_column = Column(String)
    from_date = Column(String)
    from_condition = Column(String)
    to_table = Column(String, index=True)
    to_column = Column(String)
    to_date = Column(String)
    to_id = Column(String)
    to_condition = Column(String)
    which = Column(String)
    data = Column(JSONB)
    compare_lower = Column(Integer)
    def __repr__(self):
        return "<LinkDefinition(name='%s', id='%s'>" % (
            self.name, self.id)
    
class Links(Base):
    __tablename__ = 'links'
    
    id = Column(Integer, primary_key=True)
    link_value = Column(String)
    from_date = Column(DateTime, index=True)
    to_date = Column(DateTime, index=True)
    to_id = Column(String)
    link_def = Column(String)
    data = Column(JSONB)
    def __repr__(self):
        return "<Link(link_def='%s', id='%s'>" % (
            self.link_def, self.id)
    
create_index = DDL("CREATE INDEX links_gin ON links USING gin(data);")
listen(Links.__table__, 'after_create', create_index)


class Alerts(Base):
    __tablename__ = 'alerts'

    id = Column(String, primary_key=True)
    date = Column(DateTime)
    reason = Column(String)
    clinic = Column(Integer)
    region = Column(Integer)
    data = Column(JSONB)
    uuids = Column(String)
    def __repr__(self):
        return "<Alert(reason='%s', id='%s'>" % (
            self.reason, self.id)
    
