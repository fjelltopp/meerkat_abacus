"""
Functions to calculate alerts

"""

import pandas as pd
from datetime import timedelta, datetime
import numpy as np
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
        if len(uuids) >= limits[0]:
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
        if len(uuids) >= limits[1]:
            alerts.append({
                "clinic": clinic,
                "reason": var_id,
                "duration": 7,
                "uuids": uuids,
                "type": "threshold"
            })

    return alerts


def double_double(var_id, session):
    conditions = [Data.variables.has_key(var_id)]
    data = pd.read_sql(
        session.query(Data.region, Data.district, Data.clinic, Data.date,
                      Data.uuid, Data.variables[var_id].label(var_id)).filter(
                          *conditions).statement, session.bind)

    if len(data) == 0:
        return None

    today = datetime.now()
    epi_year_weekday = epi_week_start_date(today.year).weekday()
    freq = ["W-MON", "W-TUE", "W-WED", "W-THU", "W-FRI", "W-SAT",
            "W-SUN"][epi_year_weekday]
    weekly = data.groupby(["clinic", pd.TimeGrouper(key="date", freq=freq,
                                                    label="left", closed="left")]).sum()[var_id]
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
                 cases = data[(data["clinic"] == clinic) & (data["date"] >= start_date) &
                          (data["date"] < start_date + timedelta(days=7))]
        
                 uuids = list(cases.sort(columns=["date"])["uuid"])
                 if len(uuids) > 0:
                     alerts.append({
                         "clinic": clinic,
                         "reason": var_id,
                         "duration": 7,
                         "uuids": uuids,
                         "type": "threshold"
                     })
    return alerts
