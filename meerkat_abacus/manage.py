#!/usr/bin/python3
"""
Command line tool that can be used to run all database related tasks.
"""

import argparse
from meerkat_abacus.data_management import *

parser = argparse.ArgumentParser()
parser.add_argument(
    "action",
    choices=["create-db", "import-locations", "fake-data", "import-data",
             "import-variables", "import-links", "to-codes", "add-alerts",
             "get-data-s3", "all"],
    help="Choose action")

parser.add_argument(
    "--drop-db",
    action="store_true",
    help="Use flag to drop DB for create-db or all")

parser.add_argument(
    "--leave-if-data",
    "-l",
    action="store_true",
    help="A flag for all action, if there is data "
    "in data table do nothing")

parser.add_argument(
    "-N",
    type=int,
    default=500,
    help="A flag for all actions, the number of "
    "records to create")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.action == "create-db":
        create_db(config.DATABASE_URL, model.Base, args.drop_db)
    if args.action == "import-locations":
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        import_locations(engine, session)
    if args.action == "fake-data":
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        add_fake_data(session, args.N)
    if args.action == "get-data-s3":
        if config.get_data_from_s3:
            get_data_from_s3(config.s3_bucket)
        else:
            print("Not configured for S3")
    if args.action == "import-data":
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        import_data(engine, session)
    if args.action == "import-variables":
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        import_variables(session)
    if args.action == "import-links":
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        import_links(session)
    if args.action == "to-codes":
        engine = create_engine(config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        new_data_to_codes(engine)
    if args.action == "add-alerts":
        engine = create_engine(config.DATABASE_URL, echo=True)
        Session = sessionmaker(bind=engine)
        session = Session()
        add_alerts(session)
    if args.action == "all":
        set_up_everything(args.leave_if_data, args.drop_db, args.N)
