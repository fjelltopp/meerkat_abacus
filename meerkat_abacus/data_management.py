"""
Functions to create the database, populate the db tables and proccess data.

"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
import os
import boto3
import importlib
from dateutil.parser import parse
#import resource        print(link_def.id, 'Memory usage: %s (kb)' % int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))


from meerkat_abacus.util.import_locations import import_regions
from meerkat_abacus.util.import_locations import import_clinics
from meerkat_abacus.util.import_locations import import_districts
from meerkat_abacus.util import create_fake_data, get_deviceids
from meerkat_abacus.util import write_csv, read_csv, all_location_data
import meerkat_abacus.model as model
from meerkat_abacus.model import form_tables
from meerkat_abacus.config import DATABASE_URL, country_config, data_directory, config_directory
import meerkat_abacus.config as config
import meerkat_abacus.codes.to_codes as to_codes
from meerkat_abacus import util



def create_db(url, base, country_config, drop=False):
    """
    The function creates and sets up the datbase.

    Args:
        base: An SQLAlchmey declarative base with the db schema
        url : the database_url
        country_config: A contry config dictionary
        drop: Flag to drop the database before setting it up

    Returns:
        Boolean: True
    """
    if drop and database_exists(url):
        drop_database(url)
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url)
    base.metadata.create_all(engine)
    return True


def fake_data(country_config, data_directory, engine, N=500, new=True):
    """
    Creates csv files with fake data

    Args:
        country_config: A country configuration object
        data_directory: the directory to store the from data
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    deviceids = get_deviceids(session, case_report=True)
    alert_ids = []
    forms = ["case", "register", "alert"]
    for form in country_config["tables"]:
        if form not in forms:
            forms.append(form)
    for form in forms:
        print(form)
        form_name = country_config["tables"][form]
        file_name = data_directory + form_name + ".csv"
        current_form = []
        if not new:
            current_form = read_csv(file_name)
        if "deviceids" in country_config["fake_data"][form]:
            form_deviceids = country_config["fake_data"][form]["deviceids"]
        else:
            form_deviceids = deviceids
        new_data = create_fake_data.create_form(
            country_config["fake_data"][form],
            data={"deviceids": form_deviceids, "uuids": alert_ids}, N=N)
        if form == "case":
            alert_ids = []
            for c in new_data:
                alert_ids.append(
                    c["meta/instanceID"][-country_config["alert_id_length"]:])
        write_csv(current_form + new_data, file_name)

        
def get_data_from_s3(bucket, data_directory, country_config):
    """
    Get form data from s3 bucket
    
    To run, needs to be authenticated with AWS.
    
    Args: 
       bucket: bucket_name
       data_directory: directory to store the downloaded files
       country_config: country configs to find the form files
    """
    
    s3 = boto3.resource('s3')
    for form in country_config["tables"].values():
        file_name = form + ".csv"
        repsonse = s3.meta.client.download_file(bucket,
                                                "data/"+file_name,
                                                data_directory + file_name)

    

def table_data_from_csv(filename, table, directory, session,
                        engine, deviceids=None, table_name=None, form=True,
                        row_function=None):
    """
    Adds data to table with name

    Args:
        filename: name of table
        table: table class
        directory: directory where the csv file is
        engine: SqlAlchemy engine
        session: SqlAlchemy session
        deviceids: if we should only add rows with a one of the deviceids
        table_name: name of table if different from filename
        form: if this is a form table
        row_function: function to appy to the rows before inserting
    """
    session.query(table).delete()
    if table_name != "aggregation_variables":
        if not table_name:
            engine.execute("ALTER SEQUENCE {}_id_seq RESTART WITH 1;"
                           .format(filename))
        else:
            engine.execute("ALTER SEQUENCE {}_id_seq RESTART WITH 1;"
                           .format(table_name))

    session.commit()

    for row in read_csv(directory + filename + ".csv"):
        if "_index" in row:
            row["index"] = row.pop("_index")
        if row_function:
            insert_row = row_function(row)
        else:
            insert_row = row
        if form:
            if deviceids:
                if insert_row["deviceid"] in deviceids:
                    session.add(table(**{"data": insert_row,
                                         "uuid": insert_row["meta/instanceID"]
                                     }))
            else:
                session.add(table(**{"data": insert_row,
                                     "uuid": insert_row["meta/instanceID"]}))
        else:
            insert_row.pop("")
            session.add(table(**insert_row))

    session.commit()

        
def category_to_list(row):
    """
    Transforms category to list in row
    
    Args:
        row: row of data
    Reutrns:
        row: modified row
    """
    if ";" in row["category"]:
        row["category"] = [c.strip() for c in row["category"].split(";")]
    elif "," in row["category"]:
        row["category"] = [c.strip() for c in row["category"].split(",")]
    else:
        row["category"] = [row["category"]]
    return row


def import_variables(country_config, engine):
    """
    Delete current data and then import form data
    from csv files into the database.

    Args:
        country_config: configuration
        data_directory: directory to find the forms
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()

        
    table_data_from_csv(country_config["codes_file"],
                        model.AggregationVariables,
                        config_directory,
                        session, engine,
                        table_name="aggregation_variables",
                        form=False,
                        row_function=category_to_list)


def import_data(country_config, data_directory, engine):
    """
    Delete current data and then import form data
    from csv files into the database.

    Args:
        country_config: configuration
        data_directory: directory to find the forms
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    deviceids_case = get_deviceids(session, case_report=True)
    deviceids = get_deviceids(session)
    for form in form_tables.keys():
        if form in ["case", "register"]:
            form_deviceids = deviceids_case
        else:
            form_deviceids = deviceids
        table_data_from_csv(country_config["tables"][form],
                            form_tables[form],
                            data_directory,
                            session, engine,
                            deviceids=form_deviceids)

        
def import_links(country_config, engine):
    """
    Imports all links from links-file

    Args:
        country_config: A country configuration object
        engine: SQLAlchemy connection engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    for link in config.links.links:
        session.add(model.LinkDefinitions(**link))
    session.commit()

def import_locations(country_config, config_directory, engine):
    """
    Imports all locations from csv-files

    Args:
        country_config: A country configuration object
        engine: SQLAlchemy connection engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()
    session.query(model.Locations).delete()
    engine.execute("ALTER SEQUENCE locations_id_seq RESTART WITH 1;")
    session.add(model.Locations(name=country_config["country_name"]))
    session.commit()
    regions_file = (config_directory + "locations/" +
                    country_config["locations"]["regions"])
    districts_file = (config_directory + "locations/" +
                      country_config["locations"]["districts"])
    clinics_file = (config_directory + "locations/" +
                    country_config["locations"]["clinics"])
    import_regions(regions_file, session, 1)
    import_districts(districts_file, session)
    import_clinics(clinics_file, session, 1)


def raw_data_to_variables(engine):
    """
    Turn raw data in forms into structured data with codes using
    the code from the celery app.

    Args:
        engine: db engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()

    session.query(model.Data).delete()
    engine.execute("ALTER SEQUENCE data_id_seq RESTART WITH 1;")
    session.commit()
    new_data_to_codes()
    
def add_links(engine):
    """
    Turn raw data in forms into structured data with codes using
    the code from the celery app.

    Args:
        engine: db engine
    """
    try:
        session = Session()
    except NameError:
        Session = sessionmaker(bind=engine)
        session = Session()

    session.query(model.Links).delete()
    engine.execute("ALTER SEQUENCE links_id_seq RESTART WITH 1;")
    session.commit()
    add_new_links()

    
def set_up_everything(url, leave_if_data, drop_db, N):
    """
    Set up everything by calling all the other functions

    Args:
        url: db url
        leave_if_data: do nothing if data is there
        drop_db: shall db be dropped before created
        N: number of data points to create
    """
    set_up = True
    if leave_if_data:
        if database_exists(url):
            engine = create_engine(url)
            Session = sessionmaker(bind=engine)
            session = Session()
            if len(session.query(model.Data).all()) > 0:
                set_up = False
    if set_up:
        print("Create DB")
        create_db(url, model.Base, country_config, drop=drop_db)
        engine = create_engine(url)
        Session = sessionmaker(bind=engine)
        print("Import Locations")
        import_locations(country_config, config_directory, engine)
        if config.fake_data:
            print("Generate fake data")
            fake_data(country_config, data_directory, engine, N=N)
        if config.get_data_from_s3:
            print("Get data from s3")
            get_data_from_s3(config.s3_bucket, data_directory, country_config)
        print("Import Data")
        import_data(country_config, data_directory, engine)
        print("Import Variables")
        import_variables(country_config, engine)
        print("Import Links")
        import_links(country_config, engine)
        print("To codes")
        raw_data_to_variables(engine)
        print("Add Links")
        add_links(engine)


def import_new_data():
    """
    task to check csv files and insert any new data
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    for form in model.form_tables.keys():
        file_path = data_directory + country_config["tables"][form] + ".csv"
        data = util.read_csv(file_path)
        new = util.add_new_data(form, model.form_tables[form],
                                data, session)
    return True


def add_new_fake_data(to_add):
    """
    Adds a new fake data

    Args:
       to_add: number of new records to add
    """
    engine = create_engine(DATABASE_URL)
    fake_data(country_config, data_directory, engine, to_add, new=False)


def new_data_to_codes():
    """
    add any new data in form tables to data table
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    variables = to_codes.get_variables(session)
    locations = util.all_location_data(session)
    results = session.query(model.Data.uuid)
    uuids = []
    alerts = []
    
    for row in results:
        uuids.append(row.uuid)
    for form in model.form_tables.keys():
        result = session.query(model.form_tables[form].uuid,
                               model.form_tables[form].data).yield_per(500)
        i = 0
        for row in result:
            if row.uuid not in uuids:
                new_data, alert = to_codes.to_code(
                    row.data,
                    variables,
                    locations,
                    country_config["form_dates"][form],
                    country_config["tables"][form],
                    country_config["alert_data"])
                if new_data and new_data.variables != {}:
                    session.add(new_data)
                if alert:
                    alerts.append(alert)
            i += 1
            if i % 100 == 0:
                print(i)
    add_alerts(alerts, session)
    session.commit()
    return True

def add_new_links():
    """
    add any new data in form tables to data table
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    link_defs = util.get_link_definitions(session)
    results = session.query(model.Links)

    to_ids = []
    links_by_link_value = {}
    for links_def_id in link_defs:
        links_by_link_value[links_def_id] = {}
    for row in results:
        to_ids.append(row.to_id)
        links_by_link_value[row.link_def][row.link_value] = row
    for link_def_id in link_defs.keys():
        link_def = link_defs[link_def_id]
        if "form_tables." in link_def.from_table:
            table = link_def.from_table.split(".")[1]
            from_table = model.form_tables[table]
        else:
            from_table = getattr(model, link_def.from_table)
        if link_def.from_condition:
            if ":" in link_def.from_condition:
                column, condition = link_def.from_condition.split(":")
                link_from = session.query(from_table)
                if "form_tables." in link_def.from_table:
                    query_condition = [from_table.data[column].astext == condition,
                                       from_table.data[link_def.from_column].astext != None]
                else:
                    query_condition = [getattr(from_table,column) == condition,
                                       getattr(from_table,link_def.from_column) != None]
                link_from = session.query(from_table).filter(*query_condition).yield_per(200)
            else:
                raise NameError
        else:
                link_from = session.query(from_table).yield_per(200)
        link_from_values = {}
        for row in link_from:
            if "form_tables." in link_def.from_table:
                if link_def.from_column in row.data:
                    value = row.data[link_def.from_column]
                else:
                    continue
            else:
                value = getattr(row, link_def.from_column)
            if link_def.compare_lower:
                value = value.lower()
            link_from_values[value] = row
        if link_def.to_condition:
            if ":" in link_def.to_condition:
                column, condition = link_def.to_condition.split(":")
                result_to_table = session.query(model.form_tables[link_def.to_table]).filter(
                    model.form_tables[link_def.to_table].data[column].astext == condition).yield_per(200)
            else:
                raise NameError
        else:
            result_to_table = session.query(model.form_tables[link_def.to_table]).filter(
                model.form_tables[link_def.to_table].data.has_key(link_def.to_column),
                model.form_tables[link_def.to_table].data[link_def.to_column] != None ).yield_per(200)
        for row in result_to_table:
            if row.uuid not in to_ids and link_def.to_column in row.data:
                link_to_value = row.data[link_def.to_column]
                if link_def.compare_lower:
                    link_to_value = link_to_value.lower()
                if link_to_value and link_to_value in link_from_values.keys():
                    data = sort_data(link_def.data, row.data)
                    linked_record = link_from_values[link_to_value]
                    to_date = parse(row.data[link_def.to_date])
                    if link_to_value in links_by_link_value[link_def_id].keys():
                        old_link = links_by_link_value[link_def_id][link_to_value]
                        if link_def.which == "last" and old_link.to_date <= to_date:
                            old_link.data = data
                            old_link.to_date = to_date
                            old_link.to_id = getattr(row, "uuid"),
                    else:
                        if "form_tables." in link_def.from_table:
                            from_date = linked_record.data[link_def.from_date]
                        else:
                            from_date = getattr(linked_record,
                                                 link_def.from_date)
                            
                        new_link = model.Links(**{
                            "link_value": link_to_value,
                            "to_id": getattr(row, "uuid"),
                            "to_date": to_date,
                            "from_date": from_date,
                            "link_def": link_def_id,
                            "data": data})
                        links_by_link_value[link_def_id][link_to_value] = new_link
                        session.add(new_link)
    session.commit()
    return True


def sort_data(data_def, row):
    """
    Returns row translated to data after data_def
    
    Args:
        data_def: dictionary with data definition
        row: a row to be translated
    Returns:
        data(dict): the data
    """
    data = {}
    for key in data_def.keys():
        data[key] = []
        default = None
        for value in data_def[key].keys():
            value_dict = data_def[key][value]
            # We can set a default value, if no other value is set
            if data_def[key][value]["condition"] == "default_value":
                default = value
                continue
            if isinstance(data_def[key][value]["column"], list):
                for c in data_def[key][value]["column"]:
                    if row[c] == value_dict["condition"]:
                        data[key].append(value)
                        break
            else:
                if isinstance(data_def[key][value]["condition"], list):
                    if value_dict["column"] in row and row[value_dict["column"]] in value_dict["condition"]:
                        data[key].append(value)
                else:
                    if value_dict["condition"] == "get_value":
                        data[key].append(row.get(value_dict["column"], None))
                    elif value_dict["column"] in row and value_dict["condition"] in row[value_dict["column"]].split(","):
                        data[key].append(value)
            
        if len(data[key]) == 1:
            data[key] = data[key][0]
        elif len(data[key]) == 0 and default:
            data[key] = default
    return data

                    
def add_alerts(alerts, session):
    """
    Inserts all the alerts table. If another record from the same
    day, disease and clinic already exists we create one big record.

    Args:
        alerts: list of alerts
        session: db session
    """
    # to_insert = {}
    # for alert in alerts:
    #     check = str(alert.clinic) + str(alert.reason) + str(alert.date)
    #     if check in to_insert.keys():
    #         to_insert[check].uuids = ",".join(
    #             sorted(to_insert[check].uuids.split() + [alert.uuids]))
    #     else:
    #         to_insert[check] = alert
    # results = session.query(model.Alerts)
    # for alert in results:
    #     check = str(alert.clinic) + str(alert.reason) + str(alert.date)
    #     if check in to_insert.keys():
    #         alert.uuids = ",".join(
    #             sorted(to_insert[check].uuids.split() + [alert.uuids]))
    #         alert.id = alert.uuids[-country_config["alert_id_length"]:]
    #         to_insert.pop(check, None)

    locations = util.get_locations(session)
    variables = util.get_variables(session)

    for alert in alerts:
        alert.id = alert.uuids[-country_config["alert_id_length"]:]
        util.send_alert( alert, variables, locations  )
        session.add(alert)
    
if __name__ == '__main__':
    add_new_fake_data(5)
