from meerkat_abacus import util, data_import, pipeline


def add_data_from_file(filename, form):
    """
    Adds data from filename to form

    """
    engine, session = util.get_db_engine()
    data = [row for row in util.read_csv(filename)]

    kwargs = pipeline.prepare_add_rows_arguments(form, session)
    data_import.add_rows_to_db(form, data, session, engine,
                               **kwargs)
    
        
    
