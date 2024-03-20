from datetime import datetime
from typing import Optional

import pandas as pd

from quartz_solar_forecast.data import get_nwp, make_pv_data
from quartz_solar_forecast.forecasts.tryolabs_forecast import \
    SolarPowerPredictor
from quartz_solar_forecast.forecasts.v1 import forecast_v1
from quartz_solar_forecast.pydantic_models import PVSite


def run_forecast(
    site: PVSite, ts: datetime | str = None, nwp_source: str = "icon"
) -> pd.DataFrame:
    """
    Run the forecast from NWP data

    :param site: the PV site
    :param ts: the timestamp of the site. If None, defaults to the current timestamp rounded down to 15 minutes.
    :param nwp_source: the nwp data source. Either "gfs" or "icon". Defaults to "icon"
    :return: The PV forecast of the site for time (ts) for 48 hours
    """

    # set timestamp to now if not provided
    if ts is None:
        ts = pd.Timestamp.now().round("15min")

    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)

    # make pv and nwp data from GFS
    nwp_xr = get_nwp(site=site, ts=ts)
    pv_xr = make_pv_data(site=site, ts=ts)

    # load and run models
    pred_df = forecast_v1(nwp_source, nwp_xr, pv_xr, ts)

    return pred_df


def run_xgboost_forecast(
    site: PVSite,
    model_path: str,
    ts: Optional[str] = None,
) -> pd.DataFrame:
    """
    Run the forecast using the XGBoost model

    :param site: the PV site
    :param model_path: the path to the XGBoost model
    :param ts: the start date of the forecast. If None, defaults to the current date.
    :return: The PV forecast of the site for time (ts) for 48 hours
    """
    solar_power_predictor = SolarPowerPredictor(model_path=model_path)

    if ts is None:
        start_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        start_time = pd.Timestamp.now().floor('15min')
    else:
        start_time = pd.to_datetime(ts)

    end_time = start_time + pd.Timedelta(hours=48)

    predictions = solar_power_predictor.predict_power_output(
        latitude=site.latitude,
        longitude=site.longitude,
        start_date=start_date,
        kwp=site.capacity_kwp,
        orientation=site.orientation,
        tilt=site.tilt,
    )

    predictions = predictions[(predictions['date'] >= start_time) & (predictions['date'] < end_time)]
    predictions = predictions.reset_index(drop=True)
    predictions.set_index("date", inplace=True)

    return predictions
