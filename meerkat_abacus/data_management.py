"""
Functions to create the database, populate the db tables and proccess data.

"""
from sqlalchemy import create_engine, func, and_
from sqlalchemy import exc, over, update, delete
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import bindparam
from sqlalchemy_utils import database_exists, create_database, drop_database
from dateutil.parser import parse
from datetime import datetime
from meerkat_abacus import alerts as alert_functions
from meerkat_abacus import model
from meerkat_abacus import config
from meerkat_abacus.codes import to_codes
from meerkat_abacus import util
from meerkat_abacus.util import create_fake_data, epi_week
import meerkat_libs as libs
from shapely.geometry import shape, Polygon, MultiPolygon
from geoalchemy2.shape import from_shape
import inspect
import csv
import boto3
import copy
import json
import time
import os
import os.path
import logging
import random
import subprocess

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


def add_fake_data(session, N=500, append=False, from_files=False):
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
                manual_test_data[fake_data_file] = util.read_csv(current_directory + '/test/test_data/test_cases/' +\
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
        util.write_csv(list(current_form) + list(manual_test_data_list) + generated_data, file_name)


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
                        uuid_field="meta/instanceID",
                        only_new=False,
                        deviceids=None,
                        table_name=None,
                        row_function=None,
                        quality_control=None,
                        allow_enketo=False,
                        start_dates=None,
                        exclusion_list=[],
                        fraction=None):
    """
    Adds all the data from a csv file. We delete all old data first
    and then add new data.

    If quality_control is true we look among the aggregation variables
    for variables of the import type. If this variable is not true the
    corresponding value is set to zero. If the variable has the disregard
    category we remove the whole row.

    Args:
        filename: name of table
        table: table class
        directory: directory where the csv file is
        session: SqlAlchemy session
        engine: SqlAlchemy engine
        only_new: If we should add only new data
        deviceids: if we should only add rows with a one of the deviceids
        table_name: name of table if different from filename
        row_function: function to appy to the rows before inserting
        start_dates: Clinic start dates, we do not add any data submitted
                     before these dates
        quality_control: If we are performing quality controll on the data.
        exclusion_list: A list of uuid's that are restricted from entering
        fraction: If present imports a randomly selected subset of data.
    """

    if only_new:
        result = session.query(table.uuid)
        uuids = []
        for r in result:
            uuids.append(r.uuid)
        uuids = set(uuids)
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
    to_check_test = {} # For speed
    logging.info("Filename: %s", filename)

    if quality_control:
        logging.debug("Doing Quality Control")
        (variables, variable_forms, variable_tests,
         variables_group, variables_match) = to_codes.get_variables(session, "import")
        if variables:
            to_check = [variables["import"][x][x]
                        for x in variables["import"].keys() if variables["import"][x][x].variable.form == filename]
            for variable in to_check:
                to_check_test[variable] = variable.test
    removed = {}
    for row in util.read_csv(directory + filename + ".csv"):
        if fraction:
            if random.random() > fraction:
                continue
        if row[uuid_field] in exclusion_list:
            continue # The row is in the exclusion list
        if only_new and row[uuid_field] in uuids:
            continue # In this case we only add new data
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
                try:
                    if not to_check_test[variable](insert_row):
                        if variable.variable.category == ["discard"]:
                            remove = True
                        else:
                            column = variable.column
                            if ";" in column or "," in column:
                                column = column.split(";")[0].split(",")[0]
                            category = variable.variable.category
                            replace_value = None
                            if category and len(category) > 0 and "replace:" in category[0]:
                                replace_column = category[0].split(":")[1]
                                replace_value = insert_row.get(replace_column,
                                                               None)
                            if column in insert_row:
                                insert_row[column] = replace_value
                                if insert_row[column]:
                                    if column in removed:
                                        removed[column] += 1
                                    else:
                                        removed[column] = 1
                except Exception as e:
                    logging.exception("Quality Controll error for code %s",variable.variable.id, exc_info=True)
        if remove:
            continue

        if deviceids:
            if should_row_be_added(insert_row, table_name, deviceids,
                                   start_dates, allow_enketo=allow_enketo):
                dicts.append({"data": insert_row,
                              "uuid": insert_row[uuid_field]})
                new_rows.append(insert_row[uuid_field])
        else:
            dicts.append({"data": insert_row,
                          "uuid": insert_row[uuid_field]})
            new_rows.append(insert_row[uuid_field])
        i += 1
        if i % 10000 == 0:
            logging.info("Imported batch %d.", i / 10000)
            conn.execute(table.__table__.insert(), dicts)
            dicts = []

    if to_check:
        logging.info("Quality Controll performed: ")
        logging.info("removed value: %s", removed)
    conn.execute(table.__table__.insert(), dicts)
    conn.close()
    logging.info("Number of records %s", i)
    return new_rows


def should_row_be_added(row, form_name, deviceids, start_dates, allow_enketo=False):
    """
    Determines if a data row should be added.
    If deviceid is not None, the reccord need to have one of the deviceids.
    If start_dates is not None, the record needs to be dated
    after the corresponding start date

    Args:
        row: row to be added
        form_name: name of form
        deviceids(list): the approved deviceid
        start_dates(dict): Clinic start dates
    Returns:
        should_add(Bool)
    """
    ret = False
    if deviceids is not None:
        if row.get("deviceid", None) in deviceids:
            ret = True
        else:
            if allow_enketo:
                for url in allow_enketo:
                    if url in row.get("deviceid", None):
                        ret = True
                        break
    else:
        ret = True
    if start_dates and row.get("deviceid", None) in start_dates:
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
    #check if the coding_list parameter exists. If not, use the legacy parameter codes_file instead
    if 'coding_list' in country_config.keys():
        for coding_file_name in country_config['coding_list']:
            codes_file = config.config_directory + 'variable_codes/' + coding_file_name
            for row in util.read_csv(codes_file):
                if '' in row.keys():
                    row.pop('')
                row = util.field_to_list(row, "category")
                keys = model.AggregationVariables.__table__.columns._data.keys()
                row = {key: row[key] for key in keys if key in row}
                session.add(model.AggregationVariables(**row))
            session.commit()
    else:
        codes_file = config.config_directory + country_config['codes_file'] + '.csv'
        for row in util.read_csv(codes_file):
            if '' in row.keys():
                row.pop('')
            row = util.field_to_list(row, "category")
            keys = model.AggregationVariables.__table__.columns._data.keys()
            row = {key: row[key] for key in keys if key in row}
            session.add(model.AggregationVariables(**row))
        session.commit()


def import_data(engine, session):
    """
    Imports all the data for all the forms from the csv files

    Args:
       engine: db engine
       session: db session
    """

    deviceids_case = util.get_deviceids(session, case_report=True)
    deviceids = util.get_deviceids(session)
    start_dates = util.get_start_date_by_deviceid(session)

    for form in model.form_tables.keys():

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
            model.form_tables[form],
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


def import_new_data():
    """
    Import only new data from csv files.
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
        exclusion_list = get_exclusion_list(session, form)
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
            quality_control=quality_control,
            exclusion_list=exclusion_list,
            fraction=config.import_fraction)

    return new_records


def import_clinics(csv_file, session, country_id,
                   other_info=None, other_condition=None):
    """
    Import clinics from csv file.

    Args:
        csv_file: path to csv file with clinics
        session: SQLAlchemy session
        country_id: id of the country
    """

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
                    )   # Combine case types with no duplicates
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
            name=country_config["country_name"],
            level="country",
            country_location_id=""
        )
    )

    session.query(model.Devices).delete()
    session.commit()
    zone_file = None
    if "zones" in country_config["locations"]:
        zone_file = (config.config_directory + "locations/" +
                    country_config["locations"]["zones"])
    regions_file = (config.config_directory + "locations/" +
                    country_config["locations"]["regions"])
    districts_file = (config.config_directory + "locations/" +
                      country_config["locations"]["districts"])
    clinics_file = (config.config_directory + "locations/" +
                    country_config["locations"]["clinics"])

    if zone_file:
        import_regions(zone_file, session, "zone", "country", "zone")
        import_regions(regions_file, session, "region", "zone", "region")
    else:
        import_regions(regions_file, session, "region", "country", "region")
    import_regions(districts_file, session, "district", "region", "district")
    import_clinics(clinics_file, session, 1,
                   other_info=country_config.get("other_location_information", None),
                   other_condition=country_config.get("other_location_condition", None))
    for geosjon_file in config.country_config["geojson_files"]:
        import_geojson(config.config_directory + geosjon_file,
                       session)

def import_parameters(engine, session):
    """
    Imports additional calculation parameters from csv-files.

    Args:
        engine: SQLAlchemy connection engine
        session: db session
    """
    session.query(model.CalculationParameters).delete()
    engine.execute("ALTER SEQUENCE calculation_parameters_id_seq RESTART WITH 1;")

    parameter_files = config.country_config.get("calculation_parameters",[])

    for file in parameter_files:
        logging.warning("Importing parameter file %s", file)
        file_name = os.path.splitext(file)[0]
        file_extension = os.path.splitext(file)[-1]
        if file_extension == '.json':
            with open(config.config_directory + "calculation_parameters/" +
                    file) as json_data:
                parameter_data = json.load(json_data)
                session.add(
                    model.CalculationParameters(
                        name=file_name,
                        type=file_extension,
                        parameters = parameter_data
                    ))
        elif file_extension == '.csv':
            # TODO: CSV implementation
            pass

    session.commit()


def import_dump(dump_file):
    path = config.db_dump_folder + dump_file
    logging.info("Loading DB dump: {}".format(path))
    with open(path, 'r') as f:
        command = ['psql', '-U',  'postgres', '-h', 'db', 'meerkat_db']
        proc = subprocess.Popen(command, stdin=f)
        stdout, stderr = proc.communicate()

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
        logging.info("Create DB")
        create_db(config.DATABASE_URL, drop=drop_db)
        if config.db_dump:
            import_dump(config.db_dump)
            return set_up
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        logging.info("Populating DB")
        model.Base.metadata.create_all(engine)
        logging.info("Import Locations")
        import_locations(engine, session)
        logging.info("Import calculation parameters")
        import_parameters(engine, session)
        if config.fake_data:
            logging.info("Generate fake data")
            add_fake_data(session, N=N, append=False, from_files=True)
        if config.get_data_from_s3:
            logging.info("Get data from s3")
            get_data_from_s3(config.s3_bucket)
        logging.info("Import Variables")
        import_variables(session)
        logging.info("Import Data")
        import_data(engine, session)
        logging.info("Controlling initial visits")
        initial_visit_control()
        logging.info("To codes")
        session.query(model.Data).delete()
        engine.execute("ALTER SEQUENCE data_id_seq RESTART WITH 1;")
        session.commit()

        new_data_to_codes(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        logging.info("Add alerts")
        add_alerts(session)
        logging.info("Notifying developer")
        logging.info(libs.hermes('/notify', 'PUT', data={
            'message': 'Abacus is set up and good to go for {}.'.format(
                country_config['country_name']
            )
        }))
    return set_up


def get_exclusion_list(session, form):
    """
    Get exclusion list for a form

    Args:
        session: db session
        form: which form to get the exclusion list for
    """
    exclusion_lists = config.country_config.get("exclusion_lists",{})
    ret = []


    for exclusion_list_file in exclusion_lists.get(form,[]):
        exclusion_list = util.read_csv(config.config_directory + exclusion_list_file)
        for uuid_to_be_removed in exclusion_list:
            ret.append(uuid_to_be_removed["uuid"])

    return ret


def add_alerts(session):
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

        if a.alert_type and "threshold:" in a.alert_type:
            var_id = a.id
            limits = [int(x) for x in a.alert_type.split(":")[1].split(",")]
            hospital_limits = None
            if len(limits) == 4:
                hospital_limits = limits[2:]
                limits = limits[:2]
            new_alerts = alert_functions.threshold(
                var_id,
                limits,
                session,
                hospital_limits=hospital_limits
            )
            type_name = "threshold"
        if a.alert_type == "double":
            new_alerts = alert_functions.double_double(a.id, session)
            type_name = "threshold"
            var_id = a.id

        if new_alerts:
            for new_alert in new_alerts:
                # Choose a representative record for the alert
                representative = new_alert["uuids"][0]
                others = new_alert["uuids"][1:]
                records = session.query(
                    model.Data, model.form_tables[a.form]).join(
                        (model.form_tables[a.form],
                         model.form_tables[a.form].uuid == model.Data.uuid
                         )).filter(model.Data.uuid.in_(new_alert["uuids"]))
                data_records_by_uuid = {}
                form_records_by_uuid = {}
                for r in records.all():
                    data_records_by_uuid[r[0].uuid] = r[0]
                    form_records_by_uuid[r[1].uuid] = r[1]
                new_variables = data_records_by_uuid[representative].variables

                # Update the variables of the representative alert
                new_variables["alert"] = 1
                new_variables["alert_type"] = type_name
                new_variables["alert_duration"] = new_alert["duration"]
                new_variables["alert_reason"] = var_id
                new_variables["alert_id"] = data_records_by_uuid[
                    representative].uuid[-country_config["alert_id_length"]:]

                for data_var in country_config["alert_data"][a.form].keys():
                    new_variables["alert_" + data_var] = form_records_by_uuid[
                        representative].data[country_config["alert_data"][a.form][
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

                    for data_var in country_config["alert_data"][a.form].keys():
                        data_records_by_uuid[o].variables[
                            "alert_" + data_var] = form_records_by_uuid[
                                o].data[country_config["alert_data"][a.form][data_var]]
                    flag_modified(data_records_by_uuid[o], "variables")
                session.commit()
                session.flush()
                # #send_alerts([data_records_by_uuid[representative]], session)

            new_alerts = []


def create_alert_id(alert):
    """
    Create an alert id based on the alert we have

    Args:
        alert: alert_dictionary

    returns:
       alert_id: an alert id

    """
    return "".join(sorted(alert["uuids"]))[-country_config["alert_id_length"]:]


def add_new_fake_data(to_add, from_files = False):
    """
    Wrapper function to add new fake data to the existing csv files
i
    Args:
       to_add: number of new records to add
    """
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    add_fake_data(session, to_add, append=True, from_files=from_files)


def create_links(data_type, input_conditions, table, session, conn):
    """
    Creates all the links in the Links table.

    This function uses sql queries to directly populate the links
    table with all the links without pulling the data in to python
    at all. Based on the links defined in the links file we
    generate the required sql query to create the links table.

    Args:
        data_type: The data type we are working with
        input_conditions: Some data types have conditions for
                          which records qualify
        table: Class of the table we are linking from
        session: Db session
        conn: DB connection

    """

    links_by_type, links_by_name = util.get_links(config.config_directory +
                                                  country_config["links_file"])
    link_names = []
    if data_type["type"] in links_by_type:
        for link in links_by_type[data_type["type"]]:
            conditions = list(input_conditions)
            columns = [table.uuid.label("uuid_from")]
            if link["from_form"] == data_type["form"]:
                aggregate_condition = link['aggregate_condition']
                to_form = model.form_tables[link["to_form"]]
                from_form = model.form_tables[link["from_form"]]
                link_names.append(link["name"])
                link_alias = aliased(to_form)
                columns.append(link_alias.uuid.label("uuid_to"))
                columns.append(bindparam("type", link["name"]).label("type"))
                columns.append(link_alias.data.label("data_to"))

                # split the semicolon separated join parameters into lists
                join_operators = link["method"].split(";")
                join_operands_from = link["from_column"].split(";")
                join_operands_to = link["to_column"].split(";")

                # assert that the join parameter lists are equally long
                assert len(join_operators) == len(join_operands_from)
                assert len(join_operands_from) == len(join_operands_to)

                # loop through and handle the lists of join parameters
                join_on = []
                for i in range(0, len(join_operators)):
                    if join_operators[i] == "match":
                        join_on.append(link_alias.data[
                            join_operands_to[i]].astext ==
                            table.data[join_operands_from[i]].astext)

                    elif join_operators[i] == "lower_match":
                        join_on.append(func.replace(func.lower(
                            link_alias.data[
                                join_operands_to[i]].astext), "-", "_") ==
                                func.replace(
                                    func.lower(table.data[
                                        join_operands_from[i]]
                                               .astext), "-", "_"))

                    elif join_operators[i] == "alert_match":
                        join_on.append(link_alias.data[join_operands_to[i]].astext == \
                            func.substring(
                                table.data[join_operands_from[i]].astext,
                                42 - country_config["alert_id_length"],
                                country_config["alert_id_length"]))

                    # check that the column values used for join are not empty
                    conditions.append(
                        link_alias.data[join_operands_to[i]].astext != '')
                    conditions.append(table.data[join_operands_from[i]].astext != '')

                # handle the filter condition
                if link["to_condition"]:
                    column, condition = link["to_condition"].split(":")
                    conditions.append(
                        link_alias.data[column].astext == condition)

                # make sure that the link is not referring to itself
                conditions.append(from_form.uuid != link_alias.uuid)

                # build query from join and filter conditions
                link_query = session.query(*columns).join(
                    link_alias, and_(*join_on)).filter(*conditions)
                # use query to perform insert
                insert = model.Links.__table__.insert().from_select(
                    ("uuid_from", "uuid_to", "type", "data_to"), link_query)
                conn.execute(insert)

                # split aggregate constraints into a list
                aggregate_conditions = aggregate_condition.split(';')

                # if the link type has uniqueness constraint, remove non-unique links and circular links
                if 'unique' in aggregate_conditions:
                    dupe_query = session.query(model.Links.uuid_from).\
                                            filter(model.Links.type == link["name"]).\
                                            group_by(model.Links.uuid_from).\
                                            having(func.count() > 1)


                    dupe_delete = session.query(model.Links.uuid_from).\
                        filter(model.Links.uuid_from.in_(dupe_query),
                        model.Links.type == link["name"]).\
                        delete(synchronize_session='fetch')

                    aliased_link_table = aliased(model.Links)
                    circular_query = session.query(model.Links.id).\
                                            join(aliased_link_table,and_(\
                                                model.Links.uuid_from == aliased_link_table.uuid_to,\
                                                model.Links.uuid_to == aliased_link_table.uuid_from)).\
                                            filter(model.Links.type == link["name"]).\
                                            filter(aliased_link_table.type == link["name"])

                    circular_delete = session.query(model.Links).\
                        filter(model.Links.id.in_(circular_query),
                        model.Links.type == link["name"]).\
                        delete(synchronize_session='fetch')


                session.commit()
    return link_names


def new_data_to_codes(engine=None, debug_enabled=True, restrict_uuids=None):
    """
    Run all the raw data through the to_codes
    function to translate it into structured data

    Args:
        engine: db engine
        debug_enabled: enables debug logging of operations
        restrict_uuids: If we should only update data related to
                       uuids in this list

    """

    if restrict_uuids is not None:
        if restrict_uuids == []:
            logging.info("No new data to add")
            return True
    if not engine:
        engine = create_engine(config.DATABASE_URL)

    Session = sessionmaker(bind=engine)
    session = Session()

    locations = util.all_location_data(session)

    data_types = util.read_csv(config.config_directory + country_config[
        "types_file"])
    links_by_type, links_by_name = util.get_links(config.config_directory +
                                                  country_config["links_file"])

    alerts = []
    conn = engine.connect()
    conn2 = engine.connect()
    session.query(model.Links).delete()
    session.commit()

    for data_type in data_types:
        table = model.form_tables[data_type["form"]]
        if debug_enabled:
            logging.info("Data type: %s", data_type["type"])
        variables = to_codes.get_variables(session,
                                           match_on_type=data_type["type"],
                                           match_on_form=data_type["form"])
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

        # Set up the links

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
        # We stream the results to avoid using too much memory

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
                    newly_added = data_to_db(
                        conn2, data_dicts,
                        disregarded_data_dicts,
                        data_type["type"]
                    )
                    added += newly_added
                    alerts += new_alerts
                data = {uuid: last_data}
            if debug_enabled:
                logging.info("Added %s records", added)
        if data:
            data_dicts, disregarded_data_dicts, new_alerts = to_data(
                data, link_names, links_by_name, data_type, locations,
                variables)
            newly_added = data_to_db(conn2, data_dicts,
                                     disregarded_data_dicts, data_type["type"])
            added += newly_added
            if debug_enabled:
                logging.info("Added %s records", added)
            alerts += new_alerts
    send_alerts(alerts, session)
    conn.close()
    conn2.close()

    return True

def data_to_db(conn, data_dicts, disregarded_data_dicts, data_type):
    """
    Adds a list of data_dicts to the database. We make sure we do
    not add any duplicates by deleting any possible duplicates first

    Args:
        conn: Db connection
        data_dicts: List of data dictionaries
        disregarded_data_dicts: List of date for the disregard data table
        data_type: The data typer we are adding
    Returns:
        Number of records added
    """
    if data_dicts:
        uuids = [row["uuid"] for row in data_dicts]
        conn.execute(model.Data.__table__.delete().where(
            model.Data.__table__.c.uuid.in_(uuids)).where(
                model.Data.__table__.c.type == data_type)
        )
        conn.execute(model.Data.__table__.insert(), data_dicts)
    if disregarded_data_dicts:
        uuids = [row["uuid"] for row in disregarded_data_dicts]
        conn.execute(model.DisregardedData.__table__.delete().where(
            model.DisregardedData.__table__.c.uuid.in_(uuids)).where(
                model.DisregardedData.__table__.c.type == data_type))

        conn.execute(model.DisregardedData.__table__.insert(),
                     disregarded_data_dicts)
    return len(data_dicts) + len(disregarded_data_dicts)

def to_data(data, link_names, links_by_name, data_type, locations, variables):
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

        rows = [row]
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
                sub_row[data_type["form"]][data_type["uuid"]] = sub_row[data_type["form"]][data_type["uuid"]] + ":" + str(i)
                if data_in_row:
                    sub_rows.append(sub_row)
                i += 1
            rows = sub_rows
        for row in rows:
            variable_data, category_data, location_data, disregard = to_codes.to_code(
                row, variables, locations, data_type["type"],
                data_type["form"],
                country_config["alert_data"],
                multiple_forms, data_type["location"]
                )
            if location_data is None:
                logging.warning("Missing loc data")
                continue
            try:
                date = parse(row[data_type["form"]][data_type["date"]])
                date = datetime(date.year, date.month, date.day)
            except:
                logging.error("Invalid Date: %s", row[data_type["form"]][data_type["date"]])
                continue

            # if date < locations[0][location_data["clinic"]].start_date:
            #     next
            if "alert" in variable_data:
                variable_data["alert_id"] = row[data_type["form"]][data_type[
                    "uuid"]][-country_config["alert_id_length"]:]
            variable_data[data_type["var"]] = 1
            variable_data["data_entry"] = 1
            epi_year, week = epi_week(date)
            new_data = {
                "date": date,
                "epi_week": week,
                "epi_year": epi_year,
                "type": data_type["type"],
                "uuid": row[data_type["form"]][data_type["uuid"]],
                "variables": variable_data,
                "categories": category_data,
                "links": links,
                "type_name": data_type["name"]
            }
            for l in location_data.keys():
                new_data[l] = location_data[l]
            if disregard:
                disregarded_data_rows.append(new_data)
            else:
                if "alert" in variable_data:
                    alerts.append(model.Data(**new_data))
                data_rows.append(new_data)
    return data_rows, disregarded_data_rows, alerts


def send_alerts(alerts, session):
    """
    Send alert messages

    Args:
        alerts: list of alerts
        session: db session
    """
    locations = util.get_locations(session)
    variables = util.get_variables(session)

    # Sort the alerts by date, and only use the 10 most recent.
    # To avoid accidental spamming - should never need to send more than 10.
    alerts.sort(key=lambda alert: alert.date)
    alerts = alerts[-10:]

    for alert in alerts:
        alert_id = alert.uuid[-country_config["alert_id_length"]:]
        util.send_alert(alert_id, alert, variables, locations)

def initial_visit_control():
    """
    Configures and corrects the initial visits and removes the calculated codes
    from the data table where the visit was amended
    """

    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    if "initial_visit_control" not in country_config:
        return []

    log = []
    corrected = []
    for form_table in country_config['initial_visit_control'].keys():
        table = model.form_tables[form_table]
        identifier_key_list = country_config['initial_visit_control'][form_table]['identifier_key_list']
        visit_type_key = country_config['initial_visit_control'][form_table]['visit_type_key']
        visit_date_key = country_config['initial_visit_control'][form_table]['visit_date_key']
        module_key = country_config['initial_visit_control'][form_table]['module_key']
        module_value = country_config['initial_visit_control'][form_table]['module_value']


        ret_corrected = correct_initial_visits(session, table, identifier_key_list, visit_type_key, visit_date_key,
            module_key, module_value)
        for i in ret_corrected.fetchall():
            corrected.append(i[0])
            log.append({'timestamp':str(datetime.now()),'uuid':i[0]})


    file_name = config.data_directory + 'initial_visit_control_corrected_rows.csv'
    util.write_csv(log, file_name, mode = "a")

    return corrected


def correct_initial_visits(session, table,
    identifier_key_list=['patientid','icd_code'], visit_type_key='intro./visit', visit_date_key='pt./visit_date',
    module_key='intro./module', module_value="ncd"):
    """
    Corrects cases where a patient has multiple initial visits.
    The additional initial visits will be corrected to return visits.

    Args:
        session: db session
        table: table to check for duplicates
        identifier_key_list: list of json keys in the data column that should occur only once for an initial visit
        visit_type_key: key of the json column data that defines visit type
        visit_date_key: key of the json column data that stores the visit date
        module_key: module to filter the processing to
        module_value
    """

    new_visit_value = "new"
    return_visit_value = "return"

    # construct a comparison list that makes sure the identifier jsonb data values are not empty
    identifier_column_objects = []
    empty_values_filter = []
    for key in identifier_key_list:

        # make a column object list of identifier values
        identifier_column_objects.append(table.data[key].astext)

        # construct a comparison list that makes sure the identifier
        # jsonb data values are not empty
        empty_values_filter.append(table.data[key].astext != "")

    # create a Common Table Expression object to rank visit dates accoring to
    cte_table_ranked = session.query(
        table.id, table.uuid,
        func.jsonb_set(table.data,'{'+visit_type_key+'}','"return"',False).label('data'),
        over(func.rank(),
            partition_by = [*identifier_column_objects],
            order_by =[table.data[visit_date_key],table.id]).label('rnk'))\
        .filter(table.data[visit_type_key].astext == new_visit_value)\
        .filter(and_(*empty_values_filter))\
        .filter(table.data[module_key].astext == module_value)\
        .cte("cte_table_ranked")



    # create delete statement using the Common Table Expression
    data_entry_delete = delete(model.Data).where(and_(model.Data.uuid == cte_table_ranked.c.uuid, cte_table_ranked.c.rnk > 1))

    # create update query using the Common Table Expression
    duplicate_removal_update = update(table.__table__)\
    .where(and_(table.id == cte_table_ranked.c.id, cte_table_ranked.c.rnk > 1))\
    .values(data = cte_table_ranked.c.data)\
    .returning(table.uuid)

    ret = session.execute(duplicate_removal_update)

    session.commit()

    return ret


if __name__ == "__main__":
    engine = create_engine(config.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    export_data(session)
