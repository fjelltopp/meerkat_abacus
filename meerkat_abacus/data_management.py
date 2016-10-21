"""
Functions to create the database, populate the db tables and proccess data.

"""
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.sql.expression import bindparam
from sqlalchemy_utils import database_exists, create_database, drop_database
import boto3


import csv, logging
from dateutil.parser import parse
from datetime import datetime
import inspect
# import resource                print('Memory usage: %s (kb)' % int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

from meerkat_abacus import alerts as alert_functions
from meerkat_abacus import model
from meerkat_abacus import config
from meerkat_abacus.codes import to_codes
from meerkat_abacus import util
from meerkat_abacus.util import create_fake_data
country_config = config.country_config

def create_db(url, base, drop=False):
    """
    The function creates the database

    Args:
        url : the database_url
        base: An SQLAlchmey declarative base with the db schema
        drop: Flag to drop the database before creating it

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


def export_data(session):
    for name, obj in inspect.getmembers(model):
        if inspect.isclass(obj) and hasattr(obj, "__table__"):
            for r in session.query(obj):
                columns = dict((col, getattr(r, col))
                               for col in r.__table__.columns.keys())
                print(name + "(**" + str(columns) + "),")


def add_fake_data(session, N=500, append=False):
    """
    Creates a csv file with fake data for each form. We make
    sure that the forms have deviceids that match the imported locations.

    For the case report forms we save the X last characters of
    meta/instanceID to use as alert_ids for the alert_form,
    where X is the alert_id_lenght from the config file.

    Args:
       session: SQLAlchemy session
       N: number of rows to create for each from (default=500)
       append: If we should append the new fake data or write
               over the old (default=False)
    """
    deviceids = util.get_deviceids(session, case_report=True)
    alert_ids = []
    forms = country_config["tables"]
    # Make sure the case report form is handled before the alert form
    for form in forms:
        form_name = form
        file_name = config.data_directory + form_name + ".csv"
        current_form = []
        if form not in country_config["fake_data"]:
            continue
        if append:
            current_form = util.read_csv(file_name)
        if "deviceids" in country_config["fake_data"][form]:
            # This is a special way to limit the deviceids for a form in the config file
            form_deviceids = country_config["fake_data"][form]["deviceids"]
        else:
            form_deviceids = deviceids
        new_data = create_fake_data.create_form(
            country_config["fake_data"][form], data={"deviceids":
                                                     form_deviceids,
                                                     "uuids": alert_ids}, N=N)
        if "case" in form:
            alert_ids = []
            for row in new_data:
                alert_ids.append(row["meta/instanceID"][-country_config[
                    "alert_id_length"]:])
        util.write_csv(list(current_form) + new_data, file_name)


def get_data_from_s3(bucket):
    """
    Get csv-files with data from s3 bucket

    Needs to be authenticated with AWS to run.

    Args: 
       bucket: bucket_name
    """
    s3 = boto3.resource('s3')
    for form in country_config["tables"]:
        file_name = form + ".csv"
        s3.meta.client.download_file(bucket, "data/" + file_name,
                                     config.data_directory + file_name)


def table_data_from_csv(filename,
                        table,
                        directory,
                        session,
                        engine,
                        only_new=False,
                        deviceids=None,
                        table_name=None,
                        row_function=None,
                        quality_control=None,
                        start_dates=None):
    """
    Adds all the data from a csv file. We delete all old data first and then add new data. 

    Args:
        filename: name of table
        table: table class
        directory: directory where the csv file is
        engine: SqlAlchemy engine
        session: SqlAlchemy session
        deviceids: if we should only add rows with a one of the deviceids
        table_name: name of table if different from filename
        row_function: function to appy to the rows before inserting
    """

    if only_new:
        result = session.query(table.uuid)
        uuids = []
        for r in result:
            uuids.append(r.uuid)
    else:
        session.query(table).delete()
        engine.execute("ALTER SEQUENCE {}_id_seq RESTART WITH 1;".format(
            filename))
        session.commit()

    i = 0
    dicts = []
    conn = engine.connect()
    new_rows = []
    to_check = []
    print(filename)
    if quality_control:
        print("Doing Quality Control")
        (variables, variable_forms, variable_tests,
         variables_group) = to_codes.get_variables(session, "import")
        if variables:
            to_check = [variables["import"][x][x]
                        for x in variables["import"].keys()]
    removed = {}
    for row in util.read_csv(directory + filename + ".csv"):

        if only_new and row["meta/instanceID"] in uuids:
            continue
        if "_index" in row:
            row["index"] = row.pop("_index")
        if row_function:
            insert_row = row_function(row)
        else:
            insert_row = row
        # If we have quality checks
        remove = False
        if to_check:
            for variable in to_check:
                if not variable.test(insert_row):
                    if variable.variable.category == ["discard"]:
                        remove = True
                    else:
                        if insert_row[variable.column]:
                            insert_row[variable.column] = None
                            if variable.column in removed:
                                removed[variable.column] += 1
                            else:
                                removed[variable.column] = 1

                        # Set the
        if remove:
            continue
        if deviceids:
            if should_row_be_added(insert_row, table_name, deviceids,
                                   start_dates):
                dicts.append({"data": insert_row,
                              "uuid": insert_row["meta/instanceID"]})
                new_rows.append(insert_row["meta/instanceID"])
        else:
            dicts.append({"data": insert_row,
                          "uuid": insert_row["meta/instanceID"]})
            new_rows.append(insert_row["meta/instanceID"])
        i += 1
        if i % 10000 == 0:
            conn.execute(table.__table__.insert(), dicts)
            dicts = []
        #     session.commit()
    if to_check:
        print(removed)
    conn.execute(table.__table__.insert(), dicts)
    conn.close()
    return new_rows


def should_row_be_added(row, form_name, deviceids, start_dates):
    """
    Determines if a data row should be added.
    If deviceid is not None, the reccord need to have one of the deviceids.
    If start_dates is not None, the record needs to be dated
    after the corresponding start date

    Args:
        row: row to be added
        form_name: name of form
        deviceids(dict)
        start_date(dict)
    Returns:
        should_add(Bool)
    """
    ret = False
    if deviceids is not None:
        if row["deviceid"] in deviceids:
            ret = True
    else:
        ret = True
    if start_dates and row["deviceid"] in start_dates:
        if not row["SubmissionDate"]:
            ret = False
        elif parse(row["SubmissionDate"]) < start_dates[row["deviceid"]]:
            ret = False
    return ret


def import_variables(session):
    """
    Import variables from codes csv-file.

    Args:
       session: db-session
    """
    session.query(model.AggregationVariables).delete()
    session.commit()
    codes_file = config.config_directory + country_config[
        "codes_file"] + ".csv"
    for row in util.read_csv(codes_file):
        row.pop("")
        row = util.field_to_list(row, "category")
        keys = model.AggregationVariables.__table__.columns._data.keys()
        row = {key: row[key] for key in keys if key in row}
        session.add(model.AggregationVariables(**row))
    session.commit()


def import_data(engine, session):
    """
    Imports csv-files with form data.

    Args:
       engine: db engine
       session: db session
    """

    deviceids_case = util.get_deviceids(session, case_report=True)
    deviceids = util.get_deviceids(session)
    start_dates = util.get_start_date_by_deviceid(session)

    for form in model.form_tables.keys():
        if form in country_config["require_case_report"]:
            form_deviceids = deviceids_case
        else:
            form_deviceids = deviceids
        quality_control = False
        if "quality_control" in country_config:
            if form in country_config["quality_control"]:
                quality_control = True
        table_data_from_csv(
            form,
            model.form_tables[form],
            config.data_directory,
            session,
            engine,
            deviceids=form_deviceids,
            table_name=form,
            start_dates=start_dates,
            quality_control=quality_control)


def import_new_data():
    """
    Import new data from csv files.
    """
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    new_records = []
    deviceids_case = util.get_deviceids(session, case_report=True)
    deviceids = util.get_deviceids(session)
    start_dates = util.get_start_date_by_deviceid(session)
    for form in model.form_tables.keys():
        if form in country_config["require_case_report"]:
            form_deviceids = deviceids_case
        else:
            form_deviceids = deviceids

        quality_control = False
        if "quality_control" in country_config:
            if form in country_config["quality_control"]:
                quality_control = True

        new_records += table_data_from_csv(
            form,
            model.form_tables[form],
            config.data_directory,
            session,
            engine,
            only_new=True,
            deviceids=form_deviceids,
            table_name=form,
            start_dates=start_dates,
            quality_control=quality_control)

    return new_records


def import_clinics(csv_file, session, country_id):
    """
    Import clinics from csv file.

    Args:
        csv_file: path to csv file with clinics
        session: SQLAlchemy session
        country_id: id of the country
    """

    logging.warning(country_config["default_start_date"])
    result = session.query(model.Locations)\
                    .filter(model.Locations.parent_location == country_id)
    regions = {}
    for region in result:
        regions[region.name] = region.id

    districts = {}
    result = session.query(model.Locations)\
                    .filter(model.Locations.parent_location != country_id)
    for district in result:
        districts[district.name] = district.id

    deviceids = []
    with open(csv_file) as f:
        clinics_csv = csv.DictReader(f)
        for row in clinics_csv:
            if row["deviceid"] and row["clinic"].lower() != "not used" and row[
                    "deviceid"] not in deviceids:
                if "case_report" in row.keys():
                    if row["case_report"] in ["Yes", "yes"]:
                        case_report = 1
                    else:
                        case_report = 0
                else:
                    case_report = 0

                # Prepare a device item

                if "device_tags" in row:
                    tags = row["device_tags"].split(",")
                else:
                    tags = []
                session.add(
                    model.Devices(
                        device_id=row["deviceid"], tags=tags))
                deviceids.append(row["deviceid"])
                # If the clinic has a district we use that as the parent_location,
                # otherwise we use the region
                if row["district"]:
                    parent_location = districts[row["district"]]
                elif row["region"]:
                    parent_location = regions[row["region"]]
                result = session.query(model.Locations)\
                                .filter(model.Locations.name == row["clinic"],
                                        model.Locations.parent_location == parent_location,
                                        model.Locations.clinic_type != None)

                # If two clinics have the same name and the same parent_location,
                # we are dealing with two tablets from the same clinic, so we
                # combine them.
                if len(result.all()) == 0:
                    if row["longitude"] and row["latitude"]:
                        geolocation = row["latitude"] + "," + row["longitude"]
                    else:
                        geolocation = None
                    if "start_date" in row and row["start_date"]:
                        start_date = parse(row["start_date"], dayfirst=True)
                    else:
                        start_date = country_config["default_start_date"]
                    session.add(
                        model.Locations(
                            name=row["clinic"],
                            parent_location=parent_location,
                            geolocation=geolocation,
                            deviceid=row["deviceid"],
                            clinic_type=row["clinic_type"],
                            case_report=case_report,
                            level="clinic",
                            start_date=start_date))
                else:
                    location = result.first()
                    location.deviceid = location.deviceid + "," + row[
                        "deviceid"]
    session.commit()


def import_regions(csv_file, session, parent_id):
    """
    Import regions from csv-file.

    Args:
        csv_file: path to csv file with regions
        session: SQLAlchemy session
        parent_id: The id of the country
    """
    with open(csv_file) as f:
        csv_regions = csv.DictReader(f)
        for row in csv_regions:
            session.add(
                model.Locations(
                    name=row["region"],
                    parent_location=parent_id,
                    geolocation=row["geo"],
                    level="region"))
    session.commit()


def import_districts(csv_file, session):
    """
    Import districts from csv file.

    Args:
        csv_file: path to csv file with districts
        session: SQLAlchemy session
    """
    regions = {}
    for instance in session.query(model.Locations):
        regions[instance.name] = instance.id
    with open(csv_file) as f:
        districts_csv = csv.DictReader(f)
        for row in districts_csv:
            session.add(
                model.Locations(
                    name=row["district"],
                    parent_location=regions[row["region"]],
                    level="district"))
    session.commit()


def import_locations(engine, session):
    """
    Imports all locations from csv-files.

    Args:
        engine: SQLAlchemy connection engine
        session: db session
    """
    session.query(model.Locations).delete()
    engine.execute("ALTER SEQUENCE locations_id_seq RESTART WITH 1;")
    session.add(
        model.Locations(
            name=country_config["country_name"], level="country"))

    session.query(model.Devices).delete()
    session.commit()
    regions_file = (config.config_directory + "locations/" +
                    country_config["locations"]["regions"])
    districts_file = (config.config_directory + "locations/" +
                      country_config["locations"]["districts"])
    clinics_file = (config.config_directory + "locations/" +
                    country_config["locations"]["clinics"])
    import_regions(regions_file, session, 1)
    import_districts(districts_file, session)
    import_clinics(clinics_file, session, 1)


def set_up_everything(leave_if_data, drop_db, N):
    """
    Sets up the db and imports all the data. This should leave 
    the database completely ready to used by the API.

    Args:
        leave_if_data: do nothing if data is there
        drop_db: shall db be dropped before created
        N: number of data points to create
    """
    set_up = True
    if leave_if_data:
        if database_exists(config.DATABASE_URL):
            engine = create_engine(config.DATABASE_URL)
            Session = sessionmaker(bind=engine)
            session = Session()
            if len(session.query(model.Data).all()) > 0:
                set_up = False
    if set_up:
        print("Create DB")
        create_db(config.DATABASE_URL, model.Base, drop=drop_db)
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        print("Import Locations")
        import_locations(engine, session)
        if config.fake_data:
            print("Generate fake data")
            add_fake_data(session, N=N, append=False)
        if config.get_data_from_s3:
            print("Get data from s3")
            get_data_from_s3(config.s3_bucket)
        print("Import Variables")
        import_variables(session)
        print("Import Data")
        import_data(engine, session)
        print("To codes")
        session.query(model.Data).delete()
        engine.execute("ALTER SEQUENCE data_id_seq RESTART WITH 1;")
        session.commit()

        new_data_to_codes(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        print("Add alerts")
        add_alerts(session)
        # session.query(model.Links).delete()
        # engine.execute("ALTER SEQUENCE links_id_seq RESTART WITH 1;")
        # session.commit()
        # add_new_links()
    return set_up


def add_alerts(session):
    """
    Adds alerts

    """
    alerts = session.query(model.AggregationVariables).filter(
        model.AggregationVariables.alert == 1)
    for a in alerts:
        if "threshold:" in a.alert_type:
            limits = [int(x) for x in a.alert_type.split(":")[1].split(",")]
            new_alerts = alert_functions.threshold(a.id, limits, session)
            print(new_alerts)
            for new_alert in new_alerts:
                new_alert["id"] = create_alert_id(new_alert)
                session.add(model.Alert(**new_alert))
            session.commit()


def create_alert_id(alert):
    """
    Create an alert id based on the alert we have

    Args:
        alert: alert_dictionary

    returns:
       alert_id: an alert id

    """
    return "".join(sorted(alert["uuids"]))[-country_config["alert_id_length"]:]
    
                            
            
            

def add_new_fake_data(to_add):
    """
    Wrapper function to add new fake data to the existing csv files
i
    Args:
       to_add: number of new records to add
    """
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    add_fake_data(session, to_add, append=True)


def create_links(data_type, input_conditions, table, session, conn):
    """
    Creates all the links in the Links table.

    """

    links_by_type, links_by_name = util.get_links(config.config_directory +
                                                  country_config["links_file"])
    link_names = []
    if data_type["type"] in links_by_type:
        for link in links_by_type[data_type["type"]]:
            conditions = list(input_conditions)
            columns = [table.uuid.label("uuid_from")]
            if link["from_form"] == data_type["form"]:
                to_form = model.form_tables[link["to_form"]]
                link_names.append(link["name"])
                link_alias = aliased(to_form)
                columns.append(link_alias.uuid.label("uuid_to"))
                columns.append(bindparam("type", link["name"]).label("type"))
                columns.append(link_alias.data.label("data_to"))
                if link["method"] == "match":
                    join_on = link_alias.data[link[
                        "to_column"]].astext == table.data[link[
                            "from_column"]].astext
                elif link["method"] == "alert_match":
                    join_on = link_alias.data[link[
                        "to_column"]].astext == func.substring(
                            table.data[link["from_column"]].astext,
                            42 - country_config["alert_id_length"],
                            country_config["alert_id_length"])
                if link["to_condition"]:
                    column, condition = link["to_condition"].split(":")
                    conditions.append(
                        link_alias.data[column].astext == condition)
                conditions.append(
                    link_alias.data[link["to_column"]].astext != '')
                conditions.append(table.data[link["from_column"]].astext != '')

                link_query = session.query(*columns).join(
                    (link_alias, join_on)).filter(*conditions)
                insert = model.Links.__table__.insert().from_select(
                    ("uuid_from", "uuid_to", "type", "data_to"), link_query)
                conn.execute(insert)
    return link_names


def new_data_to_codes(engine=None, no_print=False, restrict_uuids=None):
    """
    Run all the raw data through the to_codes
    function to translate it into structured data

    Args:
        engine: db engine

    """
    print(restrict_uuids)
    if restrict_uuids is not None:
        if restrict_uuids == []:
            print("No new data to add")
            return True
    if not engine:
        engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    variables = to_codes.get_variables(session)
    locations = util.all_location_data(session)
    #    old_data = session.query(model.Data.uuid)

    data_types = util.read_csv(config.config_directory + country_config[
        "types_file"])
    links_by_type, links_by_name = util.get_links(config.config_directory +
                                                  country_config["links_file"])
    #   uuids = []
    alerts = []

    conn = engine.connect()
    conn2 = engine.connect()
    session.query(model.Links).delete()
    session.commit()
    for data_type in data_types:
        table = model.form_tables[data_type["form"]]
        print(data_type["type"])
        tables = [table]
        link_names = [None]
        conditions = []

        query_condtion = []
        if data_type["db_column"]:
            query_condtion = [
                table.data[data_type["db_column"]].astext ==
                data_type["condition"]
            ]
            conditions.append(query_condtion[0])

        link_names += create_links(data_type, conditions, table, session, conn)

        # Main Query

        if restrict_uuids is not None:
            result = session.query(model.Links.uuid_from).filter(
                model.Links.uuid_to.in_(restrict_uuids))
            restrict_uuids_all = restrict_uuids + [row.uuid_from
                                                   for row in result]
            query_condtion.append(table.uuid.in_(restrict_uuids_all))

        query = session.query(
            table.uuid, table.data, model.Links.data_to,
            model.Links.type).outerjoin(
                (model.Links,
                 table.uuid == model.Links.uuid_from)).filter(*query_condtion)

        data = {}

        res = conn.execution_options(
            stream_results=True).execute(query.statement)
        print("Executed")
        added = 0

        while True:
            chunk = res.fetchmany(500)
            if not chunk:
                break
            else:
                for row in chunk:
                    uuid = row[0]
                    link_type = row[3]
                    if uuid in data and link_type:
                        data[uuid].setdefault(link_type, [])
                        data[uuid][link_type].append(row[2])
                    else:
                        data[uuid] = {}
                        data[uuid][tables[0].__tablename__] = row[1]
                        if link_type:
                            data[uuid].setdefault(link_type, [])
                            data[uuid][link_type].append(row[2])
                # Send all data apart from the latest UUID to to_data function
                last_data = data.pop(uuid)
                if data:
                    data_dicts, disregarded_data_dicts, new_alerts = to_data(
                        data, link_names, links_by_name, data_type, locations,
                        variables)
                    newly_added = data_to_db(conn2, data_dicts,
                                             disregarded_data_dicts)
                    added += newly_added
                    alerts += new_alerts
                data = {uuid: last_data}
            print("Added {} records".format(added))
        if data:
            data_dicts, disregarded_data_dicts, new_alerts = to_data(
                data, link_names, links_by_name, data_type, locations,
                variables)
            newly_added = data_to_db(conn2, data_dicts, disregarded_data_dicts)
            added += newly_added
            print("Added {} records".format(added))
            alerts += new_alerts
    if alerts:
        session.bulk_save_objects(alerts)
        session.commit()
    #send_alerts(alerts, session)
    conn.close()
    conn2.close()
    return True


def data_to_db(conn2, data_dicts, disregarded_data_dicts):
    if data_dicts:
        conn2.execute(model.Data.__table__.insert(), data_dicts)
    if disregarded_data_dicts:
        conn2.execute(model.DisregardedData.__table__.insert(),
                      disregarded_data_dicts)
    return len(data_dicts)


def to_data(data, link_names, links_by_name, data_type, locations, variables):
    """
    Constructs structured data from the entries in the data
    list. 

    """
    alerts = []
    data_rows = []
    disregarded_data_rows = []
    multiple_forms = set(link_names)
    for key, row in data.items():
        if not key:
            continue
        links = {}
        if len(row.keys()) > 1:
            for k in row.keys():
                if k in link_names:
                    # Want to correctly order the linked forms
                    column, method = links_by_name[k]["order_by"].split(";")
                    if method == "date":
                        sort_function = lambda x: parse(x[column])
                    else:
                        sort_function = lambda x: x[column]
                    row[k] = sorted(row[k], key=sort_function)
                    links[k] = [x[links_by_name[k]["uuid"]] for x in row[k]]
        variable_data, location_data, disregard = to_codes.to_code(
            row, variables, locations, data_type["type"], data_type["form"],
            country_config["alert_data"], multiple_forms)

        try:
            date = parse(row[data_type["form"]][data_type["date"]])
            date = datetime(date.year, date.month, date.day)
        except:
            print("Invalid Date", row[data_type["form"]][data_type["date"]])
            continue

        if date < locations[0][location_data["clinic"]].start_date:
            next
        if "alert" in variable_data:
            variable_data["alert_id"] = row[data_type["form"]][data_type[
                "uuid"]][-country_config["alert_id_length"]:]
            alerts.append(model.Alert(
                id=variable_data["alert_id"],
                type="individual",
                clinic = location_data["clinic"],
                reason=variable_data["alert_reason"],
                date=date,
                uuids=[row[data_type["form"]][data_type["uuid"]]]
                )
            )
        variable_data[data_type["var"]] = 1
        new_data = {
            "date": date,
            "type": data_type["type"],
            "uuid": row[data_type["form"]][data_type["uuid"]],
            "variables": variable_data,
            "links": links
        }
        for l in location_data.keys():
            new_data[l] = location_data[l]
        # if "alert" in variable_data:
        #     alerts.append(new_data)
        if disregard:
            disregarded_data_rows.append(new_data)
        else:
            data_rows.append(new_data)
    return data_rows, disregarded_data_rows, alerts


def send_alerts(alerts, session):
    """
    Inserts all the alerts. and calls the send_alert function.

    Args:
        alerts: list of alerts
        session: db session
    """
    locations = util.get_locations(session)
    variables = util.get_variables(session)
    for alert in alerts:
        alert = model.Data(**alert)
        alert_id = alert.uuid[-country_config["alert_id_length"]:]
        util.send_alert(alert_id, alert, variables, locations)


if __name__ == "__main__":
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    export_data(session)
