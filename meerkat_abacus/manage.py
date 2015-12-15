#!/usr/bin/python3
from meerkat_abacus.data_management import *

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["create-db",
                                       "import-locations",
                                       "fake-data",
                                       "import-data",
                                       "import-variables",
                                       "import-links",
                                       "to-codes",
                                       "add-links",
                                       "all"],
                    help="Choose action" )
parser.add_argument("--drop-db", action="store_true",
                    help="Use flag to drop DB for create-db or all")
parser.add_argument("--leave-if-data", "-l", action="store_true",
                    help="A flag for all action, if there is data "
                    "in data table do nothing")
parser.add_argument("-N", type=int, default=500,
                    help="A flag for all actions, the number of "
                    "records to create")


if __name__ == "__main__":
    args = parser.parse_args()

    if args.action == "create-db":
        create_db(DATABASE_URL, model.Base, country_config, drop=args.drop_db)
    if args.action == "import-locations":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_locations(country_config, engine)
    if args.action == "fake-data":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        fake_data(country_config, form_directory, engine, N=args.N)
    if args.action == "import-data":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_data(country_config, form_directory, engine)
    if args.action == "import-variables":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_variables(country_config, engine)
    if args.action == "import-links":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        import_links(country_config, engine)
    if args.action == "to-codes":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        raw_data_to_variables(engine)
    if args.action == "add-links":
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        add_links(engine)
    if args.action == "all":
        set_up_everything(DATABASE_URL,args.leave_if_data,
                          args.drop_db, args.N)
