import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import func, or_, and_

from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus.util.epi_week import epi_year_start_date
from meerkat_abacus import model
from meerkat_abacus import util


class AddMultipleAlerts(ProcessingStep):

    def __init__(self, param_config, session):
        self.step_name = "add_multiple_alerts"
        self.alerts = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.alert == 1,
            model.AggregationVariables.alert_type != "indivdual").all()

        self.locations = util.all_location_data(session)[0]
        self.config = param_config
        self.session = session
    
    def run(self, form, data):
        """
        Checks data to see if it contributes to a multiple alert.
        Currently implemented double doble and thresholds.
        
        """
        return_data = []
        for a in self.alerts:
            var_id = a.id
            if not a.alert_type:
                continue
            alert_type = a.alert_type.split(":")[0]
            if var_id not in data["variables"]:
                continue

            new_alerts = []
            type_name = None
            if alert_type == "threshold":
                new_alerts = threshold(
                    var_id,
                    a.alert_type,
                    data["date"],
                    data["clinic"],
                    self.session
                )
                type_name = "threshold"
            elif alert_type == "double":
                new_alerts = double_double(a.id,
                                           data["epi_week"],
                                           data["epi_year"],
                                           data["clinic"],
                                           self.session)
                type_name = "threshold"
            return_data += self._handle_new_alerts(new_alerts, a, type_name, form)
            if len(return_data) == 0:
                return_data.append({"form": form,
                                    "data": data})
        return return_data
        
    def _handle_new_alerts(self, new_alerts, a, type_name, form):
        return_data = []
        if new_alerts:
            for new_alert in new_alerts:
                # Choose a representative record for the alert
                others = new_alert["uuids"][1:]
                representative = new_alert["uuids"][0]
                
                form_table = model.form_tables(param_config=self.config)[a.form]
                records = self.session.query(
                    model.Data, form_table).join(
                        (form_table,
                         form_table.uuid == model.Data.uuid
                         )).filter(model.Data.uuid.in_(new_alert["uuids"]),
                                   model.Data.type == a.type)
                
                data_records_by_uuid = {}
                form_records_by_uuid = {}
                for r in records.all():
                    data_records_by_uuid[r[0].uuid] = r[0]
                    form_records_by_uuid[r[1].uuid] = r[1]
         
                new_variables = data_records_by_uuid[representative].variables

                # Update the variables of the representative alert
                new_variables["alert"] = 1
                new_variables["alert_type"] = type_name
                new_variables["alert_duration"] = new_alert["duration"]
                new_variables["alert_reason"] = a.id
                new_variables["alert_id"] = data_records_by_uuid[
                    representative].uuid[
                        -self.config.country_config["alert_id_length"]:]

                self._add_alert_data(new_variables, form_records_by_uuid[representative],
                                     a.form)
                # Update all the non-representative rows
                for o in others:
                    self._update_other_row(data_records_by_uuid[o],
                                           form_records_by_uuid[o],
                                           representative,
                                           a.form)

                for record in data_records_by_uuid.values():
                    dict_record = row_to_dict(record)
                    return_data.append({"form": form,
                                        "data": dict_record})
        return return_data

    def _update_other_row(self, row, form_record, representative, form):
        row.variables["sub_alert"] = 1
        row.variables["master_alert"] = representative
        if "alert" in row.variables:
            del row.variables["alert"]
            del row.variables["alert_id"]
        self._add_alert_data(row.variables, form_record, form)
    
    def _add_alert_data(self, variables, form_record, form):
        for data_var in self.config.country_config["alert_data"][form].keys():
            data_column = self.config.country_config["alert_data"][form][data_var]
            variables["alert_" + data_var] = form_record.data[data_column]

            
def row_to_dict(row):
    dict_record = dict((col, getattr(row, col))
                       for col in row.__table__.columns.keys())
    if dict_record.get("geolocation") is not None:
        dict_record["geolocation"] = dict_record["geolocation"].desc
    return dict_record

            
def threshold(var_id, alert_type, date, clinic, session):
    """
    Calculate threshold alerts based on daily and weekly limits

    Returns alerts for all days where there are more than limits[0] cases
    of var_id in one clinic or where ther are more than limits[1] cases of
    var_id in one clinic for one week.

    Args:
       var_id: variable id for alert
       limits: (daily, weekly) limits
       session: Db session

    Returns:
        alerts: list of alerts.
    """

    conditions = [model.Data.variables.has_key(var_id),
                  model.Data.clinic == clinic,
                  model.Data.date > date - timedelta(days=7),
                  model.Data.date < date + timedelta(days=7)]
    data = pd.read_sql(
        session.query(model.Data.region, model.Data.district,
                      model.Data.clinic, model.Data.date, model.Data.clinic_type,
                      model.Data.uuid, model.Data.variables[var_id].label(var_id)).filter(
                          *conditions).statement, session.bind)
    if len(data) == 0:
        return None
    # Group by clinic and day
    limits = [
        int(x) for x in alert_type.split(":")[1].split(",")]
    hospital_limits = None
    if len(limits) == 4:
        hospital_limits = limits[2:]
        limits = limits[:2]
    daily = data.groupby(["clinic", pd.Grouper(
        key="date", freq="1D")]).sum()[var_id]

    daily_over_threshold = daily[daily >= limits[0]] 
    alerts = []
    for clinic_date in daily_over_threshold.index:
        clinic, date = clinic_date
        data_row = data[(data["clinic"] == clinic) & (data["date"] == date)]
        if len(data_row) == 0:
            continue
        clinic_type = data_row["clinic_type"].iloc[0]
        uuids = list(data_row["uuid"])

        add = False
        if hospital_limits and clinic_type == "Hospital":
            if len(uuids) >= hospital_limits[0]:
                add = True
        else:
            if len(uuids) >= limits[0]:
                add = True
        if add:
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 1,
                "uuids": uuids,
                "type": "threshold"
            })

    today = datetime.now()
    epi_year_weekday = epi_year_start_date(today).weekday()
    freq = ["W-MON", "W-TUE", "W-WED", "W-THU", "W-FRI", "W-SAT",
            "W-SUN"][epi_year_weekday]
    # Group by clinic and epi week
    weekly = data.groupby(["clinic", pd.Grouper(
        key="date", freq=freq, label="left")]).sum()[var_id]
    weekly_over_threshold = weekly[weekly >= limits[1]]

    for clinic_date in weekly_over_threshold.index:
        clinic, date = clinic_date
        cases = data[(data["clinic"] == clinic) & (data["date"] >= date) & (
            data["date"] < date + timedelta(days=7))]
        if len(cases) == 0:
            continue
        clinic_type = cases["clinic_type"].iloc[0]
        uuids = list(cases.sort_values(["date"])["uuid"])

        add = False
        if hospital_limits and clinic_type == "Hospital":
            if len(uuids) >= hospital_limits[1]:
                add = True
        else:
            if len(uuids) >= limits[1]:
                add = True
        if add:
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 7,
                "uuids": uuids,
                "type": "threshold"
            })

    return alerts


def double_double(var_id, week, year, clinic, session):
    """
    Calculate threshold alerts based on a double doubling of cases.

    We want to trigger an alert for a clinic if there has been a doubling of cases
    in two consecutive weeks. I.e if the case numbers look like: 2, 4, 8. We would
    not trigger an alert for 2, 4, 7 or 2, 3, 8.

    Args:
       var_id: variable id for alert
       limits: (daily, weekly) limits
       session: Db session

    Returns:
        alerts: list of alerts.
    """
    #

    lower_limit = week - 2
    upper_limit = week + 2
    conditions = [model.Data.variables.has_key(var_id),
                  model.Data.clinic == clinic]

    if lower_limit >= 1 and upper_limit <= 52:
        conditions += [model.Data.epi_week >= lower_limit,
                       model.Data.epi_week <= upper_limit,
                       model.Data.epi_year == year]
    elif upper_limit <= 52:
        lower_limit = 52 + lower_limit
        conditions += [or_(and_(model.Data.epi_week >= lower_limit,
                               model.Data.epi_year == year - 1), 
                          and_(model.Data.epi_week <= upper_limit,
                               model.Data.epi_year == year))]
    else:
        upper_limit = upper_limit - 52
        conditions += [or_(and_(model.Data.epi_week >= lower_limit,
                               model.Data.epi_year == year),
                           and_(model.Data.epi_week <= upper_limit,
                               model.Data.epi_year == year + 1))]
    data = session.query(model.Data.epi_week,
                         func.count(model.Data.variables[var_id]).label(var_id),
                         func.string_agg(model.Data.uuid, ",")).filter(
                          *conditions).group_by(model.Data.epi_week).all()
    counts = {}
    uuids = {}
    s = 0
    for d in data:
        row_week = d[0]
        week_diff = row_week - week
        if abs(week_diff) > 2:
            if week_diff > 0:
                row_week = row_week - 52
            else:
                row_week = row_week + 52
        counts[row_week] = d[1]
        s += d[1]
        uuids[row_week] = d[2]
    if s < 14:
        return []

    alerts = []
    if counts.get(week, 0) > 1:
        if (counts.get(week + 1, 0) >= 2 * counts.get(week, 0) and
            counts.get(week + 2, 0) >= 2 * counts.get(week + 1, 0)):
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 7,
                "uuids": uuids[week + 2].split(","),
                "type": "threshold"
            })
    if counts.get(week - 1 , 0) > 1:
        if (counts.get(week, 0) >= 2 * counts.get(week - 1, 0) and
            counts.get(week + 1, 0) >= 2 * counts.get(week, 0)):

            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 7,
                "uuids": uuids[week + 1].split(","),
                "type": "threshold"
            })
    if counts.get(week - 2 , 0) > 1:
        if (counts.get(week - 1, 0) >= 2 * counts.get(week - 2, 0) and
            counts.get(week, 0) >= 2 * counts.get(week - 1, 0)):
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 7,
                "uuids": uuids[week].split(","),
                "type": "threshold"
            })

    return alerts
