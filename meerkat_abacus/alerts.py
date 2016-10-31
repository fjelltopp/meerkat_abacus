"""
Functions to calculate alerts

"""

import pandas as pd
from datetime import timedelta, datetime

from meerkat_abacus.model import Data
from meerkat_abacus.util import epi_week_start_date


def threshold(var_id, limits, session):
    conditions = [Data.variables.has_key(var_id)]
    data = pd.read_sql(
        session.query(Data.region, Data.district, Data.clinic, Data.date,
                      Data.uuid, Data.variables[var_id].label(var_id)).filter(
                          *conditions).statement, session.bind)

    if len(data) == 0:
        return None
    daily = data.groupby(["clinic", pd.TimeGrouper(
        key="date", freq="1D")]).sum()[var_id]

    daily_over_threshold = daily[daily >= limits[0]]

    alerts = []

    for clinic_date in daily_over_threshold.index:
        clinic, date = clinic_date
        uuids = list(data[(data["clinic"] == clinic) & (data["date"] == date)][
            "uuid"])
        if len(uuids) > limits[1]:
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 1,
                "uuids": uuids,
                "type": "threshold"
            })

    today = datetime.now()
    epi_year_weekday = epi_week_start_date(today.year).weekday()
    freq = ["W-MON", "W-TUE", "W-WED", "W-THU", "W-FRI", "W-SAT",
            "W-SUN"][epi_year_weekday]
    weekly = data.groupby(["clinic", pd.TimeGrouper(
        key="date", freq=freq, label="left")]).sum()[var_id]
    weekly_over_threshold = weekly[weekly >= limits[1]]
    for clinic_date in weekly_over_threshold.index:
        clinic, date = clinic_date

        cases = data[(data["clinic"] == clinic) & (data["date"] >= date) &
                          (data["date"] < date + timedelta(days=7))]
        
        uuids = list(cases.sort(columns=["date"])["uuid"])
        if len(uuids) > limits[1]:
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 7,
                "uuids": uuids,
                "type": "threshold"
            })

    return alerts
