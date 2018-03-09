import logging


from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model


class WriteToDb(ProcessingStep):

    def __init__(self, param_config, session, engine):
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
        self.data_to_write = {}
        self.data_to_delete = {}

    def end_step(self, n):
        conn = self.config["engine"].connect()
        for table in self.data_to_delete.keys():
            for condition, uuids in self.data_to_delete[table].items():
                conn.execute(table.__table__.delete().where(
                    table.__table__.c.uuid.in_(uuids)).where(
                        getattr(
                            table.__table__.c, self.config["delete"][table]) ==
                        condition))

        for table in self.data_to_write.keys():
            conn.execute(table.__table__.insert(), self.data_to_write[table])
        self.data_to_write = {}
        self.data_to_delete = {}

        super(WriteToDb, self).end_step(n)
        
    def run(self, form, data):
        """
        Write to db
        
        """
        table = self.config["form_to_table"][form]
        if form in self.config["raw_data_forms"]:
            insert_data = {"uuid": get_uuid(data, form, self.config),
                           "data": data}
        else:
            insert_data = data
            
        if form in self.config["delete"]:
            uuid = data["uuid"]
            other_condition = data[self.config["delete"][form]]
            self.config["delete"][table] = self.config["delete"][form]
            self.data_to_delete.setdefault(table, {})
            self.data_to_delete[table].setdefault(other_condition, [])
            self.data_to_delete[table][other_condition].append(uuid)
            
        if data:
            if "id" in data:
                del data["id"]
            self.data_to_write.setdefault(table, [])
            self.data_to_write[table].append(insert_data)
        return [{"form": form,
                 "data": data}]

    
def get_uuid(data, form, config):
    uuid_field = "meta/instanceID"
    if "tables_uuid" in config["country_config"]:
        uuid_field = config["country_config"]["tables_uuid"].get(form, uuid_field)
    return data[uuid_field]

