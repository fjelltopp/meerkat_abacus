"""
Functions to create the database, populate the db tables and proccess data.

"""
import copy
import csv
import inspect
import json
import logging
import os
import os.path
import random
import subprocess
import time

import boto3
from datetime import datetime
from dateutil.parser import parse
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, Polygon, MultiPolygon
from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy import exc, over, update, delete
from sqlalchemy.orm import sessionmaker, aliased, Query
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import bindparam
from sqlalchemy_utils import database_exists, create_database, drop_database
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


from meerkat_abacus.util import data_types
import meerkat_libs as libs
from meerkat_abacus import alerts as alert_functions
from meerkat_abacus.config import config
from meerkat_abacus import model
from meerkat_abacus import util
from meerkat_abacus.codes import to_codes
from meerkat_abacus.util import create_fake_data
from meerkat_abacus.util.epi_week import epi_week_for_date
from meerkat_libs import consul_client as consul

country_config = config.country_config

def create_db(url, drop=False):
    """
    The function creates the database

    Args:
        url : the database_url
        base: An SQLAlchmey declarative base with the db schema
        drop: Flag to drop the database before creating it

    Returns:
        Boolean: True
    """
    counter = 0
    while counter < 5:
        try:
            if drop and database_exists(url):
                logging.debug('Dropping database.')
                drop_database(url)
            if not database_exists(url):
                logging.debug('Creating database.')
                create_database(url)
                break

        except exc.OperationalError:
            logging.exception('There was an error connecting to the db.', exc_info=True)
            logging.error('Trying again in 5 seconds...')
            time.sleep(5)
            counter = counter + 1

    engine = create_engine(url)
    connection = engine.connect()
    connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    connection.close()
    return True


def export_data(session):
    """
    Helper function to export all the data in the database.
    Prints out all the objects

    Args:
       session: db_session
    """
    for name, obj in inspect.getmembers(model):
        if inspect.isclass(obj) and hasattr(obj, "__table__"):
            for r in session.query(obj):
                columns = dict((col, getattr(r, col))
                               for col in r.__table__.columns.keys())
                logging.debug(name + "(**" + str(columns) + "),")

def add_fake_data(session, N=500, append=False,
                  from_files=False, param_config=config,
                  write_to="file"):
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
       from_files: whether to add data from the manual test case
                   files defined in country_config
    """
    logging.debug("fake data")
    deviceids = util.get_deviceids(session, case_report=True)
    alert_ids = []
    country_config = param_config.country_config
    forms = country_config["tables"]
    # Make sure the case report form is handled before the alert form
    for form in forms:
        logging.debug("Processing form: %s", form)
        file_name = config.data_directory + form + ".csv"
        current_form = []
        if form not in country_config["fake_data"]:
            continue
        if append:
            current_form = util.read_csv(file_name)
        if "deviceids" in country_config["fake_data"][form]:
            # This is a special way to limit the deviceids for a form in
            # the config file
            form_deviceids = country_config["fake_data"][form]["deviceids"]
        else:
            form_deviceids = deviceids

        manual_test_data = {}
        if from_files and form in country_config.get("manual_test_data", {}).keys():
            current_directory = os.path.dirname(os.path.realpath(__file__))
            for fake_data_file in country_config.get("manual_test_data", {})[form]:
                manual_test_data[fake_data_file] = []
                logging.debug("Adding test data from file: %s.csv", fake_data_file)
                manual_test_data[fake_data_file] = util.read_csv(current_directory + '/test/test_data/test_cases/' + \
                                                                 fake_data_file + ".csv")

        generated_data = create_fake_data.create_form(
            country_config["fake_data"][form], data={"deviceids":
                                                         form_deviceids,
                                                     "uuids": alert_ids}, N=N)

        if "case" in form:
            alert_ids = []
            for row in generated_data:
                alert_ids.append(row["meta/instanceID"][-country_config[
                    "alert_id_length"]:])
        manual_test_data_list = []
        for manual_test_data_file in manual_test_data.keys():
            manual_test_data_list += list(manual_test_data[manual_test_data_file])
        for row in manual_test_data_list:
            if len(generated_data) > 0:
                for key in generated_data[0].keys():
                    if key not in row:
                        row[key] = None
        data_to_write = list(current_form) + list(manual_test_data_list) + generated_data
        if write_to == "file":
            util.write_csv(data_to_write, file_name)
        elif write_to == "local_db":
            util.write_to_db(data_to_write, form,
                             param_config.PERSISTENT_DATABASE_URL,
                             param_config=param_config)



def import_variables(session, param_config=config):
    """
    Import variables from codes csv-file.

    Args:
       session: db-session
    """
    session.query(model.AggregationVariables).delete()
    session.commit()

    country_config = param_config.country_config
    # check if the coding_list parameter exists. If not, use the legacy parameter codes_file instead
    if 'coding_list' in country_config.keys():
        for coding_file_name in country_config['coding_list']:
            codes_file = param_config.config_directory + 'variable_codes/' + coding_file_name
            for row in util.read_csv(codes_file):
                if '' in row.keys():
                    row.pop('')
                row = util.field_to_list(row, "category")
                keys = model.AggregationVariables.__table__.columns._data.keys()
                row = {key: row[key] for key in keys if key in row}
                session.add(model.AggregationVariables(**row))
            session.commit()
    else:
        codes_file = param_config.config_directory + param_config.country_config['codes_file'] + '.csv'
        for row in util.read_csv(codes_file):
            if '' in row.keys():
                row.pop('')
            row = util.field_to_list(row, "category")
            keys = model.AggregationVariables.__table__.columns._data.keys()
            row = {key: row[key] for key in keys if key in row}
            session.add(model.AggregationVariables(**row))
        session.commit()


def import_data(engine, session, param_config=config):
    """
    Imports all the data for all the forms from the csv files

    Args:
       engine: db engine
       session: db session
    """

    deviceids_case = util.get_deviceids(session, case_report=True)
    deviceids = util.get_deviceids(session)
    start_dates = util.get_start_date_by_deviceid(session)

    country_config = param_config.country_config

    for form in model.form_tables().keys():

        uuid_field = "meta/instanceID"
        if "tables_uuid" in country_config:
            uuid_field = country_config["tables_uuid"].get(form, uuid_field)
        if form in country_config["require_case_report"]:
            form_deviceids = deviceids_case
        else:
            form_deviceids = deviceids
        if "no_deviceid" in country_config and form in country_config["no_deviceid"]:
            form_deviceids = []
        quality_control = False
        if "quality_control" in country_config:
            if form in country_config["quality_control"]:
                quality_control = True
        allow_enketo = False
        if form in country_config.get("allow_enketo", []):
            allow_enketo = country_config["allow_enketo"][form]
        exclusion_list = get_exclusion_list(session, form)
        table_data_from_csv(
            form,
            model.form_tables()[form],
            config.data_directory,
            session,
            engine,
            uuid_field=uuid_field,
            deviceids=form_deviceids,
            table_name=form,
            start_dates=start_dates,
            quality_control=quality_control,
            allow_enketo=allow_enketo,
            exclusion_list=exclusion_list,
            fraction=config.import_fraction)


def import_clinics(csv_file, session, country_id,
                   other_info=None, other_condition=None, param_config=config):
    """
    Import clinics from csv file.

    Args:
        csv_file: path to csv file with clinics
        session: SQLAlchemy session
        country_id: id of the country
    """
    country_config = param_config.country_config

    result = session.query(model.Locations)
    regions = {}
    for region in result:
        if region.level == "region":
            regions[region.name] = region.id
    districts = {}
    for district in result:
        if district.level == "district":
            districts[district.name] = district.id

    deviceids = []
    with open(csv_file) as f:
        clinics_csv = csv.DictReader(f)
        for row in clinics_csv:
            if row["deviceid"] and row["clinic"].lower() != "not used" and row[
                "deviceid"] not in deviceids:

                other_cond = True
                if other_condition:
                    for key in other_condition.keys():
                        if row.get(key, None) and row[key] != other_condition[key]:
                            other_cond = False
                            break
                if not other_cond:
                    continue
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

                # If the clinic has a district we use that as
                # the parent_location, otherwise we use the region
                parent_location = 1
                if row["district"].strip():
                    parent_location = districts[row["district"].strip()]
                elif row["region"].strip():
                    parent_location = regions[row["region"].strip()]

                # Add population to the clinic and add it up through
                # All the other locations
                population = 0
                if "population" in row and row["population"]:
                    population = int(row["population"])
                    pop_parent_location = parent_location
                    while pop_parent_location:
                        r = session.query(model.Locations).filter(
                            model.Locations.id == pop_parent_location).first()
                        r.population += population
                        pop_parent_location = r.parent_location
                        session.commit()

                result = session.query(model.Locations).filter(
                    model.Locations.name == row["clinic"],
                    model.Locations.parent_location == parent_location,
                    model.Locations.clinic_type is not None
                )

                # Construct other information from config
                other = {}
                if other_info:
                    for field in other_info:
                        other[field] = row.get(field, None)

                # Case type can be a comma seperated list.
                case_type = row.get("case_type", "")
                case_type = list(map(str.strip, case_type.split(',')))

                # If two clinics have the same name and the same
                # parent_location, we are dealing with two tablets from the
                # same clinic, so we combine them.
                if len(result.all()) == 0:
                    if row["longitude"] and row["latitude"]:
                        point = "POINT(" + row["longitude"] + " " + row["latitude"] + ")"
                    else:
                        point = None
                    if "start_date" in row and row["start_date"]:
                        start_date = parse(row["start_date"], dayfirst=True)
                    else:
                        start_date = country_config["default_start_date"]

                    session.add(
                        model.Locations(
                            name=row["clinic"],
                            parent_location=parent_location,
                            point_location=point,
                            deviceid=row["deviceid"],
                            clinic_type=row["clinic_type"].strip(),
                            case_report=case_report,
                            case_type=case_type,
                            level="clinic",
                            population=population,
                            other=other,
                            service_provider=row.get("service_provider", None),
                            start_date=start_date,
                            country_location_id=row.get(
                                "country_location_id",
                                None
                            )
                        )
                    )
                else:
                    location = result.first()
                    location.deviceid += "," + row["deviceid"]
                    location.case_type = list(
                        set(location.case_type) | set(case_type)
                    )  # Combine case types with no duplicates
    session.commit()


def import_geojson(geo_json, session):
    with open(geo_json) as f:
        geometry = json.loads(f.read())
        for g in geometry["features"]:
            shapely_shapes = shape(g["geometry"])
            if shapely_shapes.geom_type == "Polygon":
                coords = list(shapely_shapes.exterior.coords)
                if len(coords[0]) == 3:
                    shapely_shapes = Polygon([xy[0:2] for xy in list(coords)])
                shapely_shapes = MultiPolygon([shapely_shapes])
            elif shapely_shapes.geom_type == "MultiPolygon":
                new_polys = []
                for poly in shapely_shapes.geoms:
                    coords = list(poly.exterior.coords)
                    new_poly = Polygon([xy[0:2] for xy in list(coords)])
                    new_polys.append(new_poly)
                shapely_shapes = MultiPolygon(new_polys)
            else:
                logging.info("shapely_shapes.geom_type : %s", shapely_shapes.geom_type)
            name = g["properties"]["Name"]
            location = session.query(model.Locations).filter(
                model.Locations.name == name,
                model.Locations.level.in_(["district",
                                           "region", "country"])).first()
            if location is not None:
                location.area = from_shape(shapely_shapes)
            session.commit()


def import_regions(csv_file, session, column_name,
                   parent_column_name, level_name):
    """
    Import districts from csv file.

    Args:
        csv_file: path to csv file with districts
        session: SQLAlchemy session
    """
    parents = {}
    for instance in session.query(model.Locations):
        parents[instance.name] = instance.id
    with open(csv_file) as f:
        districts_csv = csv.DictReader(f)
        for row in districts_csv:
            session.add(
                model.Locations(
                    name=row[column_name],
                    parent_location=parents[row[parent_column_name].strip()],
                    level=level_name,
                    population=row.get("population", 0),
                    country_location_id=row.get("country_location_id", None)
                )
            )

    session.commit()


def import_locations(engine, session, param_config=config):
    """
    Imports all locations from csv-files.

    Args:
        engine: SQLAlchemy connection engine
        session: db session
    """
    country_config = param_config.country_config

    session.query(model.Locations).delete()
    engine.execute("ALTER SEQUENCE locations_id_seq RESTART WITH 1;")
    session.add(
        model.Locations(
            name=param_config.country_config["country_name"],
            level="country",
            country_location_id="the_country_location_id"
        )
    )

    session.query(model.Devices).delete()
    session.commit()
    zone_file = None
    if "zones" in country_config["locations"]:
        zone_file = (param_config.config_directory + "locations/" +
                     country_config["locations"]["zones"])
    regions_file = (param_config.config_directory + "locations/" +
                    country_config["locations"]["regions"])
    districts_file = (param_config.config_directory + "locations/" +
                      country_config["locations"]["districts"])
    clinics_file = (param_config.config_directory + "locations/" +
                    country_config["locations"]["clinics"])

    if zone_file:
        import_regions(zone_file, session, "zone", "country", "zone")
        import_regions(regions_file, session, "region", "zone", "region")
    else:
        import_regions(regions_file, session, "region", "country", "region")
    import_regions(districts_file, session, "district", "region", "district")
    import_clinics(clinics_file, session, 1,
                   other_info=param_config.country_config.get("other_location_information", None),
                   other_condition=param_config.country_config.get("other_location_condition", None),
                   param_config=param_config)
    for geosjon_file in param_config.country_config["geojson_files"]:
        import_geojson(param_config.config_directory + geosjon_file,
                       session)

def import_parameters(engine, session, param_config=config):
    """
    Imports additional calculation parameters from csv-files.

    Args:
        engine: SQLAlchemy connection engine
        session: db session
    """
    session.query(model.CalculationParameters).delete()
    engine.execute("ALTER SEQUENCE calculation_parameters_id_seq RESTART WITH 1;")

    parameter_files = param_config.country_config.get("calculation_parameters",[])

    for file in parameter_files:
        logging.debug("Importing parameter file %s", file)
        file_name = os.path.splitext(file)[0]
        file_extension = os.path.splitext(file)[-1]
        if file_extension == '.json':
            with open(param_config.config_directory + "calculation_parameters/" +
                    file) as json_data:
                parameter_data = json.load(json_data)
                session.add(
                    model.CalculationParameters(
                        name=file_name,
                        type=file_extension,
                        parameters=parameter_data
                    ))
        elif file_extension == '.csv':
            # TODO: CSV implementation
            pass

    session.commit()


def import_dump(dump_file):
    path = config.db_dump_folder + dump_file
    logging.info("Loading DB dump: {}".format(path))
    with open(path, 'r') as f:
        command = ['psql', '-U', 'postgres', '-h', 'db', 'meerkat_db']
        proc = subprocess.Popen(command, stdin=f)
        stdout, stderr = proc.communicate()


def set_up_persistent_database(param_config):
    """
    Sets up the test persistent db if it doesn't exist yet.
    """
    logging.info("Create Persistent DB")
    if not database_exists(param_config.PERSISTENT_DATABASE_URL):
        create_db(param_config.PERSISTENT_DATABASE_URL, drop=False)
        engine = create_engine(param_config.PERSISTENT_DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        logging.info("Creating persistent database tables")
        model.form_tables(param_config=param_config)
        model.Base.metadata.create_all(engine)
        engine.dispose()


def set_up_database(leave_if_data, drop_db, param_config=config):
    """
    Sets up the db and imports static data.

    Args:
        leave_if_data: do nothing if data is there
        drop_db: shall db be dropped before created
        param_config: config object for Abacus in case the function is called in a Celery container
    """
    set_up = True
    if leave_if_data:
        if database_exists(param_config.DATABASE_URL):
            engine = create_engine(param_config.DATABASE_URL)
            Session = sessionmaker(bind=engine)
            session = Session()
            if len(session.query(model.Data).all()) > 0:
                set_up = False
    if set_up:
        logging.info("Create DB")
        create_db(param_config.DATABASE_URL, drop=drop_db)
        if param_config.db_dump:
            import_dump(param_config.db_dump)
            return set_up
        engine = create_engine(param_config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        logging.info("Populating DB")
        model.form_tables(param_config=param_config)
        model.Base.metadata.create_all(engine)

        links, links_by_name = util.get_links(param_config.config_directory +
                            param_config.country_config["links_file"])

        for link in links_by_name.values():
            form_1 = link["to_form"]
            column_1 = link["to_column"]
            form_2 = link["from_form"]
            column_2 = link["from_column"]
            column_3 = link["to_condition"].split(";")[0]
            if link["method"] == "lower_match":
                column_1 = "lower(" + column_1 + ")"
                column_2 = "lower(" + column_2 + ")"
                
            engine.execute(f"CREATE index on {form_1} ((data->>'{column_1}'))")
            engine.execute(f"CREATE index on {form_2} ((data->>'{column_2}'))")
            if column_3:
                engine.execute(f"CREATE index on {form_1} ((data->>'{column_3}'))")
        

        
        logging.info("Import Locations")
        import_locations(engine, session, param_config=param_config)
        logging.info("Import calculation parameters")
        import_parameters(engine, session, param_config=param_config)
        logging.info("Import Variables")
        import_variables(session, param_config=param_config)
    return session, engine


def add_alerts(session, newely_inserted_data, param_config=config):
    """
    Adds non indivdual alerts.

    Individual alerts are added during the add data process.
    For any type of alert based on more than one case we add those
    alerts here.


    For each variable that should trigger a form of "threshold" alert.
    We calculate which records should make up the alert.
    We then choose the earliest alert as the representative of the whole
    alert. All the others are linked to it.

    TODO: We need to figure out a better way of dealing with the representative
    alert as there could be multiple alerts from the same day etc. Maybe we
    could generate alert_id from clinic name + week or date. Due to this
    issue we are currently not sending any threshold alert messages.

    Args:
        session: db_session


    """
    alerts = session.query(model.AggregationVariables).filter(
        model.AggregationVariables.alert == 1)

    

    
    for a in alerts.all():
        new_alerts = []
        data_type = a.type

        for newly_inserted in newly_inserted_data:
            var_id = a.id
            if var_id not in newly_inserted["variables"]:
                continue
            if not a.alert_type or not a.alert_type in ["threshold:
                limits = [int(x) for x in a.alert_type.split(":")[1].split(",")]
                hospital_limits = None
                if len(limits) == 4:
                    hospital_limits = limits[2:]
                    limits = limits[:2]

                    
                new_alerts = alert_functions.threshold(
                    var_id,
                    limits,
                    session,
                    day,
                    week,
                    hospital_limits=hospital_limits
                    )
                type_name = "threshold"
            if a.alert_type == "double":
                new_alerts = alert_functions.double_double(a.id, day,
                                                           week, clinic,
                                                           session)
                type_name = "threshold"
            

        if new_alerts:
            for new_alert in new_alerts:
                # Choose a representative record for the alert
                others = new_alert["uuids"][1:]
                records = session.query(
                    model.Data, model.form_tables(param_config=param_config)[a.form]).join(
                        (model.form_tables(param_config=param_config)[a.form],
                         model.form_tables(param_config=param_config)[a.form].uuid == model.Data.uuid
                         )).filter(model.Data.uuid.in_(new_alert["uuids"]),
                                   model.Data.type == data_type)
                data_records_by_uuid = {}
                form_records_by_uuid = {}
                for r in records.all():
                    data_records_by_uuid[r[0].uuid] = r[0]
                    form_records_by_uuid[r[1].uuid] = r[1]

                for uuid in new_alert["uuids"]:
                    if uuid in data_records_by_uuid:
                        representative = uuid
                        new_variables = data_records_by_uuid[representative].variables
                        break
                else:
                    return None

                # Update the variables of the representative alert
                new_variables["alert"] = 1
                new_variables["alert_type"] = type_name
                new_variables["alert_duration"] = new_alert["duration"]
                new_variables["alert_reason"] = var_id
                new_variables["alert_id"] = data_records_by_uuid[
                    representative].uuid[-param_config.country_config["alert_id_length"]:]

                for data_var in param_config.country_config["alert_data"][a.form].keys():
                    new_variables["alert_" + data_var] = form_records_by_uuid[
                        representative].data[param_config.country_config["alert_data"][a.form][
                            data_var]]

                # Tell sqlalchemy that we have changed the variables field
                data_records_by_uuid[representative].variables = new_variables
                flag_modified(data_records_by_uuid[representative],
                              "variables")
                # Update all the non-representative rows
                for o in others:
                    data_records_by_uuid[o].variables[
                        "sub_alert"] = 1
                    data_records_by_uuid[o].variables[
                        "master_alert"] = representative

                    for data_var in param_config.country_config["alert_data"][a.form].keys():
                        data_records_by_uuid[o].variables[
                            "alert_" + data_var] = form_records_by_uuid[
                            o].data[param_config.country_config["alert_data"][a.form][data_var]]
                    flag_modified(data_records_by_uuid[o], "variables")
                session.commit()
                session.flush()
                # #send_alerts([data_records_by_uuid[representative]], session)

            new_alerts = []


def create_alert_id(alert, param_config=config):
    """
    Create an alert id based on the alert we have

    Args:
        alert: alert_dictionary

    returns:
       alert_id: an alert id

    """
    return "".join(sorted(alert["uuids"]))[-param_config.country_config["alert_id_length"]:]

import time

def add_new_fake_data(to_add, from_files=False, param_config=config):
    """
    Wrapper function to add new fake data to the existing csv files
i
    Args:
       to_add: number of new records to add
    """
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    add_fake_data(session=session, N=to_add, append=True, from_files=from_files, param_config=param_config)


def create_links(links, data, base_form, form, uuid, connection, 
                 param_config=config):
    """
    Creates all the links for a given data row
    Args:
        data_type: The data type we are working with
        input_conditions: Some data types have conditions for
                          which records qualify
        table: Class of the table we are linking from
        session: Db session
        conn: DB connection

    """
    country_config = param_config.country_config

    link_names = []
    original_form = data
    if not base_form:
        for link in links:
            if link["to_form"] != form:
                continue
            if link["to_condition"]:
                column, condition = link["to_condition"].split(":")
                if original_form[column] != condition:
                    continue
            # aggregate_condition = link['aggregate_condition']
            from_form = model.form_tables(param_config=param_config)[link["from_form"]]
            link_names.append(link["name"])

            columns = [from_form.uuid, from_form.data]
            conditions = []
            for i in range(len(link["from_column"].split(";"))):
                from_column = link["from_column"].split(";")[i]
                to_column = link["to_column"].split(";")[i]
                operator = link["method"].split(";")[i]
                if operator == "match":
                    conditions.append(from_form.data[from_column].astext ==
                                      original_form[to_column])

                elif operator == "lower_match":
                    conditions.append(
                        func.replace(func.lower(from_form.data[from_column].astext),
                                                   "-", "_") ==
                                     str(original_form.get(to_column)).lower().replace("-", "_"))
                    
                elif operator == "alert_match":
                    conditions.append(func.substring(
                                           from_form.data[from_column].astext,
                                           42 - country_config["alert_id_length"],
                                           country_config["alert_id_length"]) ==
                                      original_form[to_column])

                conditions.append(from_form.data[from_column].astext != '')
            conditions.append(from_form.uuid != uuid)

            # handle the filter condition
            
            link_query = Query(*columns).filter(*conditions)
            link_query = connection.execute(link_query.query).all()
            if len(link_query) > 1:
                logging.info(link_query)
            if len(link_query) == 0:
                return None, {}
            data = link_query[0][1]

    link_data = {}
    for link in links:
        to_form = model.form_tables(param_config=param_config)[link["to_form"]]
        link_names.append(link["name"])

        columns = [to_form.uuid, to_form.data]
        conditions = []
        for i in range(len(link["from_column"].split(";"))):
            from_column = link["from_column"].split(";")[i]
            to_column = link["to_column"].split(";")[i]
            operator = link["method"].split(";")[i]
            if operator == "match":
                conditions.append(to_form.data[to_column].astext ==
                                  data[from_column])

            elif operator == "lower_match":
                conditions.append(
                    func.replace(func.lower(to_form.data[to_column].astext),
                                               "-", "_") ==
                                 str(data[from_column]).lower().replace("-", "_"))

            elif operator == "alert_match":
                conditions.append(to_form.data[to_column].astext == \
                                  data[from_column][-country_config["alert_id_length"]:])
            conditions.append(
                   to_form.uuid != uuid)
            conditions.append(to_form.data[to_column].astext != '')

        # handle the filter condition
        if link["to_condition"]:
            column, condition = link["to_condition"].split(":")
            conditions.append(
                to_form.data[column].astext == condition)

        link_query = Query(*columns).filter(*conditions)
        link_query = connection.execute(link_query).all()
        if len(link_query) > 1:
            # Want to correctly order the linked forms
            column, method = link["order_by"].split(";")
            if method == "date":
                sort_function = lambda x: parse(x[1][column])
            else:
                sort_function = lambda x: x[1][column]
            link_query = sorted(link_query, key=sort_function)
        if len(link_query) > 0:
            link_data[link["name"]] = link_query
    return data, link_data
            

def check_data_type_condition(data_type, data):
    if data_type["db_column"] and data:
        if data[data_type["db_column"]] == data_type["condition"]:
            return True
    else:
        return True
    return False

def new_data_to_codes(form, row, uuid,
                      locations,
                      links,
                      variables,
                      session,
                      engine,
                      debug_enabled=True,
                      param_config=config):
    """
    Run all the raw data through the to_codes
    function to translate it into structured data

    Args:
        engine: db engine
        debug_enabled: enables debug logging of operations
        restrict_uuids: If we should only update data related to
                       uuids in this list

    """
    country_config = param_config.country_config
    links_by_type, links_by_name = links

    data_dicts = []
    disregarded = []
    data_type_return = []
    for data_type in data_types.data_types(param_config=param_config):
        main_form = data_type["form"]
        additional_forms = []
        for link in links_by_type.get(data_type["name"], []):
            additional_forms.append(link["to_form"])
        new_data = False
        if form == main_form:
            if not check_data_type_condition(data_type, row):
                continue
            new_data = True
        elif form not in additional_forms:
            continue
#        logging.info(f"{data_type}, {form}, {main_form}, {new_data}")
        if debug_enabled:
            logging.debug("Data type: %s", data_type["type"])
        base_row, linked_records = create_links(links_by_type.get(data_type["name"], []),
                                                row, new_data, form,
                                                uuid,
                                                engine.connect(), param_config)
        if base_row is None:
            continue
        if not check_data_type_condition(data_type, base_row):
            continue
        combined_data = {main_form: base_row,
                         "links": linked_records}
        data_dict, disregarded_row = to_data(
                        combined_data, links_by_name, data_type, locations,
                        variables, session, param_config=param_config)
        for i in range(len(data_dict)):
            data_dicts.append(data_dict[i])
            disregarded.append(disregarded_row[i])
            data_type_return.append(data_type["type"])

    return data_dicts, disregarded, data_type_return


def to_data(data, links_by_name, data_type, locations, variables, session,
            param_config=config):
    """
    Constructs structured data from the entries in the data list.
    We pass the data row with all its links through the to_codes function
    to generate the list of codes for this row. We then prepare the data
    for insertion

    Args:
        data: list of data rows
        link_names: list of link names
        links_by_name: dictionary of link defs by name
        data_type: The current data type
        locations: Locations dictionary
        variables: Dict of variables

    Returns:
        data_dicts: Data to add to the Data table
        disregarded_data_dicts: Data to add to the diregarded data table
        alerts: Any new alerts added

    """
    data_rows = []

    rows = [data]
    disregarded_list = []
    if data_type["multiple_row"]:
        fields = data_type["multiple_row"].split(",")
        i = 1
        data_in_row = True
        sub_rows = []
        while data_in_row:
            data_in_row = False
            sub_row = copy.deepcopy(row)
            for f in fields:
                column_name = f.replace("$", str(i))
                sub_row_name = f.replace("$", "")
                value = row[data_type["form"]].get(column_name, None)
                if value and value != "":
                    sub_row[data_type["form"]][sub_row_name] = value
                    data_in_row = True
            sub_row[data_type["form"]][data_type["uuid"]] = sub_row[data_type["form"]][
                data_type["uuid"]] + ":" + str(i)
            if data_in_row:
                sub_rows.append(sub_row)
            i += 1
        rows = sub_rows
    for row in rows:
        multiple_forms = set(row["links"].keys())
        variable_data, category_data, location_data, disregard = to_codes.to_code(
            row, variables, locations, data_type["type"],
            data_type["form"],
            param_config.country_config["alert_data"],
            multiple_forms, data_type["location"]
        )
        if location_data is None:
            logging.warning("Missing loc data")
            continue
        try:
            date = parse(row[data_type["form"]][data_type["date"]])
            date = datetime(date.year, date.month, date.day)
            epi_year, week = epi_week_for_date(date, param_config=param_config.country_config)
        except KeyError:
            logging.error("Missing Date field %s", data_type["date"])
            continue
        except ValueError:
            logging.error(f"Failed to convert date to epi week. uuid: {row.get('uuid', 'UNKNOWN')}")
            logging.debug(f"Faulty row date: {date}.")
            continue
        except:
            logging.error("Invalid Date: %s", row[data_type["form"]].get(data_type["date"]))
            continue

        if "alert" in variable_data:
            variable_data["alert_id"] = row[data_type["form"]][data_type[
                "uuid"]][-param_config.country_config["alert_id_length"]:]
        variable_data[data_type["var"]] = 1
        variable_data["data_entry"] = 1
        submission_date = None
        if "SubmissionDate" in row[data_type["form"]]:
            submission_date = parse(row[data_type["form"]].get("SubmissionDate")).replace(tzinfo=None)

        links = {}
        for name in data["links"].keys():
            links[name] = [x[0] for x in data["links"][name]]
        new_data = {
            "date": date,
            "epi_week": week,
            "epi_year": epi_year,
            "submission_date": submission_date,
            "type": data_type["type"],
            "uuid": row[data_type["form"]][data_type["uuid"]],
            "variables": variable_data,
            "categories": category_data,
            "links": links,
            "type_name": data_type["name"]
        }
        new_data.update(location_data)
        if "alert" in variable_data and not disregard:
            alert_id = new_data["uuid"][-param_config.country_config["alert_id_length"]:]
            util.send_alert(alert_id, new_data,
                            variables, locations, param_config)
        data_rows.append(new_data)
        disregarded_list.append(disregard)
    return data_rows, disregarded_list


def initial_visit_control(form, data, engine, session, param_config=config):
    """
    Configures and corrects the initial visits and removes the calculated codes
    from the data table where the visit was amended
    """
    if "initial_visit_control" not in param_config.country_config:
        return [data]

    log = []
    corrected = []
    new_visit_value = "new"
    return_visit_value = "return"
    if form in param_config.country_config['initial_visit_control'].keys():
        table = model.form_tables(param_config=param_config)[form]
        
        identifier_key_list = param_config.country_config['initial_visit_control'][form]['identifier_key_list']

        current_identifier_values = {}
        for key in identifier_key_list:
            if data[key] is None:
                return [data]
            current_identifier_values[key] = data[key]
        visit_type_key = param_config.country_config['initial_visit_control'][form]['visit_type_key']
        if data[visit_type_key] != new_visit_value:
            return [data]
        
        visit_date_key = param_config.country_config['initial_visit_control'][form]['visit_date_key']
        module_key = param_config.country_config['initial_visit_control'][form]['module_key']
        module_value = param_config.country_config['initial_visit_control'][form]['module_value']

        if data[module_key] != module_value:
            return [data]
        ret_corrected = get_initial_visits(session, table,
                                           current_identifier_values,
                                           identifier_key_list,
                                           visit_type_key,
                                           visit_date_key,
                                           module_key, module_value)

        if len(ret_corrected) > 0:
            combined_data = [data] + [r.data for r in ret_corrected]
            combined_data.sort(key=lambda x: parse(data[visit_date_key]))
            for row in combined_data[1:]:
                row[visit_type_key] = return_visit_value
            return combined_data
        else:
            return [data]    
            
    else:
        return [data]
    #file_name = config.data_directory + 'initial_visit_control_corrected_rows.csv'
    #util.write_csv(log, file_name, mode="a")

    return corrected


def get_initial_visits(session, table, current_values,
                           identifier_key_list=['patientid', 'icd_code'],
                           visit_type_key='intro./visit',
                           visit_date_key='pt./visit_date',
                           module_key='intro./module', module_value="ncd"):
    """
    Finds cases where a patient has multiple initial visits.

    Args:
        session: db session
        table: table to check for duplicates
        current_values: current_values for identifier_keys
        identifier_key_list: list of json keys in the data column that should occur only once for an initial visit
        visit_type_key: key of the json column data that defines visit type
        visit_date_key: key of the json column data that stores the visit date
        module_key: module to filter the processing to
        module_value
    """

    new_visit_value = "new"
    return_visit_value = "return"

    # construct a comparison list that makes sure the identifier jsonb data values are not empty
    empty_values_filter = []
    conditions = []
    for key in identifier_key_list:
        # make a column object list of identifier values
        conditions.append(table.data[key].astext == current_values[key])

        # construct a comparison list that makes sure the identifier
        # jsonb data values are not empty
        empty_values_filter.append(table.data[key].astext != "")

    # create a Common Table Expression object to rank visit dates accoring to
    results = session.query(
        table.id, table.uuid,
        table.data) \
        .filter(table.data[visit_type_key].astext == new_visit_value) \
        .filter(and_(*empty_values_filter)) \
        .filter(table.data[module_key].astext == module_value)\
        .filter(*conditions)
    return results.all()


if __name__ == "__main__":
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    export_data(session)
