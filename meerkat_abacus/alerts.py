"""
Functions to calculate alerts

"""

import numpy as np
import pandas as pd
from datetime import timedelta, datetime

from meerkat_abacus.model import Data
from meerkat_abacus.util.epi_week import epi_year_start_date


def threshold(var_id, limits, session, hospital_limits=None):
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

    conditions = [Data.variables.has_key(var_id)]
    data = pd.read_sql(
        session.query(Data.region, Data.district, Data.clinic, Data.date, Data.clinic_type,
                      Data.uuid, Data.variables[var_id].label(var_id)).filter(
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


def double_double(var_id, session):
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
    conditions = [Data.variables.has_key(var_id)]
    data = pd.read_sql(
        session.query(Data.region, Data.district, Data.clinic, Data.date,
                      Data.uuid, Data.variables[var_id].label(var_id)).filter(
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
