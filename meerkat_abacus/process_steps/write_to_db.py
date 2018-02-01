import logging


from meerkat_abacus import model


def write_to_db(form, data, param_config, config):
    """
    Write to db

    """
    table = config["form_to_table"][form]
    conn = config["engine"].connect()
    if config["delete"]:
        conn.execute(table.__table__.delete().where(
            table.__table__.c.uuid == data["uuid"]).where(
                getattr(table.__table__.c, config["delete"][0]) == config["delete"][1])
        )
 
    if data:
        conn.execute(table.__table__.insert(), [data])
    conn.close()
    return [{"form": form,
             "data": data}]
    

def prepare_config(param_config, engine):
    config = {
        "delete": False,
        "engine": engine,
        "form_to_table": {
            "data": model.Data,
            "disregardedData": model.DisregardedData}
        }
    config["form_to_table"].update(model.form_tables(param_config))
    return config
