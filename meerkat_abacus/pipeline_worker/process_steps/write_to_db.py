import logging


from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model


class WriteToDb(ProcessingStep):

    def __init__(self, param_config, engine, session):
        self.step_name = "write_to_db"
        config = {
            "delete": {"data": "type",
                       "disregardedData": "type"},
            "engine": engine,
            "form_to_table": {
                "data": model.Data,
                "disregardedData": model.DisregardedData
            },
            "country_config": param_config.country_config

        }
        config["form_to_table"].update(model.form_tables(param_config))
        config["raw_data_forms"] = param_config.country_config["tables"]
        self.config = config
        self.session = session
        
    
    def run(self, form, data):
        """
        Write to db
        
        """
        table = self.config["form_to_table"][form]
        conn = self.config["engine"].connect()
        if form in self.config["raw_data_forms"]:
            insert_data = {"uuid": get_uuid(data, form, self.config),
                           "data": data}
        else:
            insert_data = data
        if form in self.config["delete"]:
            conn.execute(table.__table__.delete().where(
                table.__table__.c.uuid == data["uuid"]).where(
                    getattr(
                        table.__table__.c, self.config["delete"][form]) ==
                    data[self.config["delete"][form]])
            )
 
        if data:
            conn.execute(table.__table__.insert(), [insert_data])
        conn.close()
        return [{"form": form,
                 "data": data}]

    
def get_uuid(data, form, config):
    uuid_field = "meta/instanceID"
    if "tables_uuid" in config["country_config"]:
        uuid_field = config["country_config"]["tables_uuid"].get(form, uuid_field)
    return data[uuid_field]

