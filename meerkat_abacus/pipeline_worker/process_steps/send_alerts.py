from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model
from meerkat_abacus import util
from meerkat_abacus.config import config

logger = config.logger


class SendAlerts(ProcessingStep):

    def __init__(self, param_config, session):
        self.step_name = "send_alerts"
        alerts = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.alert == 1)

        self.alert_variables = {a.id: a for a in alerts}
        self.locations = util.all_location_data(session)[0]
        self.config = param_config
        self.session = session

    def run(self, form, data):
        """
        Send alerts
        
        """
        if ("alert" in data["variables"] and
            data["variables"]["alert_type"] == "individual"):
            alert_id = data["uuid"][
                -self.config.country_config["alert_id_length"]:]
            data["variables"]["alert_id"] = alert_id
            util.send_alert(alert_id, data,
                            self.alert_variables,
                            self.locations, self.config)
        return [{"form": form,
                "data": data}]

