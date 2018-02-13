import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus.util.epi_week import epi_year_start_date
from meerkat_abacus import model
from meerkat_abacus import util
from geoalchemy2.shape import to_shape, from_shape
from shapely.geos import WKTWriter


class AddMultipleAlerts(ProcessingStep):

    def __init__(self, param_config, session):

        self.alerts = session.query(model.AggregationVariables).filter(
            model.AggregationVariables.alert == 1)

        self.locations = util.all_location_data(session)[0]
        self.config = param_config
        self.session = session
    
    def run(self, form, data):
        """
        Write to db
        
        """
        return_data = []

        for a in self.alerts:
            new_alerts = []
            data_type = a.type
            var_id = a.id
            if not a.alert_type:
                continue
            alert_type = a.alert_type.split(":")[0]
            if var_id not in data["variables"]:
                continue
            if alert_type not in ["threshold", "double"]:
                continue
            day = data["date"]
            clinic = data["clinic"]
            if alert_type == "threshold":
                limits = [
                    int(x) for x in a.alert_type.split(":")[1].split(",")]
                hospital_limits = None
                if len(limits) == 4:
                    hospital_limits = limits[2:]
                    limits = limits[:2]
                new_alerts = threshold(
                    var_id,
                    limits,
                    day,
                    clinic,
                    self.session,
                    hospital_limits=hospital_limits
                )
            type_name = "threshold"
            if alert_type == "double":
                new_alerts = double_double(a.id, day,
                                           clinic,
                                           self.session)
                type_name = "threshold"

            if new_alerts:
                for new_alert in new_alerts:
                    # Choose a representative record for the alert
                    others = new_alert["uuids"][1:]
                    form_table = model.form_tables(param_config=self.config)[a.form]
                    records = self.session.query(
                        model.Data, form_table).join(
                            (form_table,
                             form_table.uuid == model.Data.uuid
                             )).filter(model.Data.uuid.in_(new_alert["uuids"]),
                                       model.Data.type == data_type)
                    data_records_by_uuid = {}
                    form_records_by_uuid = {}
                    for r in records.all():
                        data_records_by_uuid[r[0].uuid] = r[0]
                        form_records_by_uuid[r[1].uuid] = r[1]

                    for uuid in new_alert["uuids"]:
                        if uuid in data_records_by_uuid:
                            representative = uuid
                            new_variables = data_records_by_uuid[representative].variables
                            break
                        else:
                            return [{"data": data,
                                     "form": form}]

                    # Update the variables of the representative alert
                    new_variables["alert"] = 1
                    new_variables["alert_type"] = type_name
                    new_variables["alert_duration"] = new_alert["duration"]
                    new_variables["alert_reason"] = var_id
                    new_variables["alert_id"] = data_records_by_uuid[
                        representative].uuid[
                            -self.config.country_config["alert_id_length"]:]

                    for data_var in self.config.country_config[
                            "alert_data"][a.form].keys():
                        new_variables[
                            "alert_" + data_var] = form_records_by_uuid[
                                representative].data[
                                    self.config.country_config[
                                        "alert_data"][a.form][data_var]]

                    # Tell sqlalchemy that we have changed the variables field
                    data_records_by_uuid[representative].variables = new_variables
                  
                    # Update all the non-representative rows
                    for o in others:
                        data_records_by_uuid[o].variables[
                            "sub_alert"] = 1
                        data_records_by_uuid[o].variables[
                            "master_alert"] = representative

                        for data_var in self.config.country_config["alert_data"][a.form].keys():
                            data_records_by_uuid[o].variables[
                                "alert_" + data_var] = form_records_by_uuid[
                                o].data[self.config.country_config["alert_data"][a.form][data_var]]

                    for record in data_records_by_uuid.values():
                        dict_record = dict((col, getattr(record, col))
                                           for col in record.__table__.columns.keys())
                        if dict_record["geolocation"] is not None:
                            dict_record["geolocation"] = dict_record["geolocation"].desc
                        return_data.append({"form": form,
                                            "data": dict_record})
                new_alerts = []
        if len(return_data) == 0:
            return_data.append({"form": form,
                                "data": data})
        return return_data


def threshold(var_id, limits, date, clinic, session, hospital_limits=None):
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
    
    daily = data.groupby(["clinic", pd.TimeGrouper(
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
    weekly = data.groupby(["clinic", pd.TimeGrouper(
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


def double_double(var_id, date, clinic, session):
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
    conditions = [model.Data.variables.has_key(var_id),
                  model.Data.clinic == clinic,
                  model.Data.date > date - timedelta(days=21),
                  model.Data.date < date + timedelta(days=21)]
    data = pd.read_sql(
        session.query(model.Data.region, model.Data.district, model.Data.clinic, model.Data.date,
                      model.Data.uuid, model.Data.variables[var_id].label(var_id)).filter(
                          *conditions).statement, session.bind)

    if len(data) == 0:
        return None

    today = datetime.now()
    epi_year_weekday = epi_year_start_date(today).weekday()
    freq = ["W-MON", "W-TUE", "W-WED", "W-THU", "W-FRI", "W-SAT",
            "W-SUN"][epi_year_weekday]
    weekly = data.groupby(["clinic", pd.TimeGrouper(
        key="date", freq=freq, label="left", closed="left")]).sum()[var_id]
    alerts = []
    for clinic in weekly.index.get_level_values(level=0).unique():
        clinic_ts = weekly[clinic].resample(freq).sum().fillna(0)
        compare_series = clinic_ts.shift(periods=1, freq=freq)[:-1]
        compare_series_2 = clinic_ts.shift(periods=2, freq=freq)[:-2]
        factor = clinic_ts / compare_series
        factor2 = compare_series / compare_series_2
        factor[1:][compare_series <= 1] = 0
        factor2[1:][compare_series_2 <= 1] = 0
        is_alert = (factor >= 2) & (factor2 >= 2)
        if np.sum(is_alert):
            start_dates = clinic_ts.index[is_alert]
            for start_date in start_dates:
                cases = data[(data["clinic"] == clinic) & (data[
                    "date"] >= start_date) & (data["date"] < start_date +
                                              timedelta(days=7))]

                uuids = list(cases.sort_values(["date"])["uuid"])
                if len(uuids) > 0:
                    alerts.append({
                        "clinic": clinic,
                        "reason": var_id,
                        "duration": 7,
                        "uuids": uuids,
                        "type": "threshold"
                    })
    return alerts
