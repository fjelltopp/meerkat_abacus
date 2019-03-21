import time
import csv
import json
import os

from dateutil.parser import parse
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, Polygon, MultiPolygon
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from meerkat_abacus.util import data_types
from meerkat_abacus.config import config
from meerkat_abacus import model
from meerkat_abacus import util

logger = config.logger


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
                logger.debug('Dropping database.')
                drop_database(url)
            if not database_exists(url):
                logger.debug('Creating database.')
                create_database(url)
                break

        except exc.OperationalError:
            logger.exception('There was an error connecting to the db.', exc_info=True)
            logger.error('Trying again in 5 seconds...')
            time.sleep(5)
            counter = counter + 1

    engine = create_engine(url)
    connection = engine.connect()
    connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    connection.close()
    return True


def import_variables(session, param_config):
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


def import_clinics(csv_file, session, country_id, param_config,
                   other_info=None, other_condition=None):
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
                logger.info("shapely_shapes.geom_type : %s", shapely_shapes.geom_type)
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


def import_locations(engine, session, param_config):
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

        
def import_parameters(engine, session, param_config):
    """
    Imports additional calculation parameters from csv-files.

    Args:
        engine: SQLAlchemy connection engine
        session: db session
    """
    session.query(model.CalculationParameters).delete()
    engine.execute("ALTER SEQUENCE calculation_parameters_id_seq RESTART WITH 1;")

    parameter_files = param_config.country_config.get("calculation_parameters", [])

    for file in parameter_files:
        logger.debug("Importing parameter file %s", file)
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
    logger.info("Loading DB dump: {}".format(path))
    with open(path, 'r') as f:
        command = ['psql', '-U', 'postgres', '-h', 'db', 'meerkat_db']
        proc = subprocess.Popen(command, stdin=f)
        stdout, stderr = proc.communicate()


def set_up_persistent_database(param_config):
    """
    Sets up the test persistent db if it doesn't exist yet.
    """
    logger.info("Create Persistent DB")
    if not database_exists(param_config.PERSISTENT_DATABASE_URL):
        create_db(param_config.PERSISTENT_DATABASE_URL, drop=False)
        engine = create_engine(param_config.PERSISTENT_DATABASE_URL)
        logger.info("Creating persistent database tables")
        model.form_tables(param_config=param_config)
        model.Base.metadata.create_all(engine)
        engine.dispose()


def set_up_database(leave_if_data, drop_db, param_config):
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
        logger.info("Create DB")
        create_db(param_config.DATABASE_URL, drop=drop_db)
        if param_config.db_dump:
            import_dump(param_config.db_dump)
            return set_up
        engine = create_engine(param_config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        logger.info("Populating DB")
        model.form_tables(param_config)
        model.Base.metadata.create_all(engine)

        links, links_by_name = util.get_links(param_config.config_directory +
                                              param_config.country_config["links_file"])

        indexes_already_created = {}
        for link in links_by_name.values():
            to_form = link["to_form"]
            to_condition_column = link["to_condition"].split(":")[0]
            add_index(to_form, to_condition_column, indexes_already_created, engine)
            from_form = link["from_form"]
            from_condition_column = link.get("from_condition", "").split(":")[0]
            add_index(from_form, from_condition_column, indexes_already_created, engine)
        logger.info("Import Locations")
        import_locations(engine, session, param_config)
        logger.info("Import calculation parameters")
        import_parameters(engine, session, param_config)
        logger.info("Import Variables")
        import_variables(session, param_config)
        for alert in session.query(model.AggregationVariables).filter(
                model.AggregationVariables.alert == 1).all():
            alert_type = alert.alert_type.split(":")[0]
            if alert_type in ["threshold", "double"]:
                engine.execute(f"CREATE index on data ((variables->>'{alert.id}'))")
        
    return session, engine


def unlogg_tables(form_tables, engine):
    for table in ["data", "disregarded_data"] + form_tables:
        engine.execute(f"ALTER TABLE {table} SET UNLOGGED;")


def logg_tables(form_tables, engine):
    for table in ["data", "disregarded_data"] + form_tables:
        engine.execute(f"ALTER TABLE {table} SET LOGGED;")

        
def add_index(form, column, already_created, engine):
    if column and column not in already_created.get(form, []):
        engine.execute(f"CREATE index on {form} ((data->>'{column}'))")
        already_created.setdefault(form, [])
        already_created[form].append(column)
