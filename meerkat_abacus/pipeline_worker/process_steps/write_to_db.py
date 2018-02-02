import logging


from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model


class WriteToDb(ProcessingStep):

    def __init__(self, param_config, engine):
        config = {
            "delete": False,
            "engine": engine,
            "form_to_table": {
                "data": model.Data,
                "disregardedData": model.DisregardedData}
        }
        config["form_to_table"].update(model.form_tables(param_config))
        self.config = config
    
    def run(self, form, data):
        """
        Write to db
        
        """
        table = self.config["form_to_table"][form]
        conn = self.config["engine"].connect()
        if self.config["delete"]:
            conn.execute(table.__table__.delete().where(
                table.__table__.c.uuid == data["uuid"]).where(
                    getattr(table.__table__.c, self.config["delete"][0]) == self.config["delete"][1])
            )
 
        if data:
            conn.execute(table.__table__.insert(), [data])
        conn.close()
        return [{"form": form,
                 "data": data}]
