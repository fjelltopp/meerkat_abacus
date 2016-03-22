"""Import locations from csv"""
import csv

from meerkat_abacus.model import Locations


def import_clinics(csv_file, session, country_id):
    """
    import fistricts from csv

    Args:
        csv_file: path to csv file with regions
        session: SQLAlchemy session
        country_id: id of the country
    """

    result = session.query(Locations)\
                    .filter(Locations.parent_location == country_id)
    regions = {}
    for region in result:
        regions[region.name] = region.id

    districts = {}
    result = session.query(Locations)\
                    .filter(Locations.parent_location != country_id)
    for district in result:
        districts[district.name] = district.id

    f = open(csv_file)
    clinics_csv = csv.DictReader(f)
    for row in clinics_csv:
        if row["deviceid"]:
            if "case_report" in row.keys():
                if row["case_report"] == "Yes":
                    case_report = 1
                else:
                    case_report = 0
            else:
                case_report = 0

            if row["district"]:
                parent_location = districts[row["district"]]
            elif row["region"]:
                parent_location = regions[row["region"]]
            result = session.query(Locations)\
                            .filter(Locations.name == row["clinic"],
                                    Locations.parent_location == parent_location,
                                    Locations.clinic_type != None)
       
            if len(result.all()) == 0:
                if row["longitude"] and row["latitude"]:
                    geolocation = row["latitude"] + "," + row["longitude"]
                else:
                    geolocation = None

                session.add(Locations(name=row["clinic"],
                                      parent_location=parent_location,
                                      geolocation=geolocation,
                                      deviceid=row["deviceid"],
                                      clinic_type=row["clinic_type"],
                                      case_report=case_report,
                                      level="clinic"))
            else:
                location = result.first()
                location.deviceid = location.deviceid + "," + row["deviceid"]
    session.commit()


def import_regions(csv_file, session, parent_id):
    """
    import regions from csv

    Args:
        csv_file: path to csv file with regions
        session: SQLAlchemy session
        parent_id: The id of the country
    """
    f = open(csv_file)
    csv_regions = csv.DictReader(f)
    for row in csv_regions:
        session.add(Locations(name=row["region"],
                              parent_location=parent_id,
                              geolocation=row["geo"],
                              level="region"))
    session.commit()


def import_districts(csv_file, session):
    """
    import fistricts from csv

    Args:
        csv_file: path to csv file with regions
        session: SQLAlchemy session
    """
    regions = {}
    for instance in session.query(Locations):
        regions[instance.name] = instance.id
    f = open(csv_file)
    districts_csv = csv.DictReader(f)
    for row in districts_csv:
        session.add(Locations(name=row["district"],
                              parent_location=regions[row["region"]],
                              level="district"))
    session.commit()

