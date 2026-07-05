"""Thin client over the Open-Meteo forecast API (no key required).

One call per city, at the city's representative point (bbox centroid),
returns hourly weather for the past WEATHER_PAST_HOURS and the next
WEATHER_FORECAST_HOURS. Timestamps are requested in UTC so they align
exactly with OpenAQ's `datetime.utc` — the shared `ts_hour` join key.

Past hours matter too, not just future ones: an hour first stored as a
forecast (is_forecast=true) gets overwritten with the observed value on
a later run, so weather_hourly converges to actuals as time passes.
"""
from __future__ import annotations

import datetime as dt

import requests

from ingest.src import config

# Open-Meteo field -> our weather_hourly column. One field per dispersion
# mechanism (see the weather_hourly DDL in infra/postgres/schema.sql for the
# meteorological rationale). `wind_speed_unit=ms` below converts gusts too.
# Note: temperature_180m is served by the live /forecast endpoint but NOT by
# the ERA5 archive, so it comes back null for backfilled history — the column
# is kept (live keeps collecting it; inversion_strength becomes usable once
# enough live history accrues), it's just null in the backfilled past.
FIELDS = {
    "temperature_2m": "temperature_c",
    "temperature_180m": "temperature_180m_c",        # inversion probe (live only)
    "relative_humidity_2m": "relative_humidity",
    "dew_point_2m": "dew_point_c",
    "wind_speed_10m": "wind_speed_ms",
    "wind_direction_10m": "wind_direction_deg",
    "wind_gusts_10m": "wind_gusts_ms",
    "boundary_layer_height": "boundary_layer_height_m",  # mixing volume
    "surface_pressure": "surface_pressure_hpa",
    "cloud_cover": "cloud_cover_pct",
    "shortwave_radiation": "shortwave_radiation_wm2",
    "precipitation": "precipitation_mm",
}


def _fetch_hourly(city: str, extra_params: dict, url: str | None = None) -> list[dict]:
    """Shared fetch/parse: one record per hour with our column names."""
    cfg = config.CITIES[city]
    resp = requests.get(
        url or f"{config.open_meteo_base()}/forecast",
        params={
            "latitude": cfg["lat"],
            "longitude": cfg["lon"],
            "hourly": ",".join(FIELDS),
            "wind_speed_unit": "ms",
            "timezone": "UTC",
            **extra_params,
        },
        timeout=config.REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    hourly = resp.json().get("hourly", {})

    times = hourly.get("time", [])
    now = dt.datetime.now(dt.timezone.utc)
    records: list[dict] = []
    for i, iso in enumerate(times):
        ts = dt.datetime.fromisoformat(iso).replace(tzinfo=dt.timezone.utc)
        rec = {
            "city": city,
            "ts_hour": ts.isoformat(),
            "is_forecast": ts > now,
        }
        for api_field, column in FIELDS.items():
            values = hourly.get(api_field) or []
            rec[column] = values[i] if i < len(values) else None
        records.append(rec)
    return records


def fetch_city_weather(city: str) -> list[dict]:
    """Live pull: past 24 h observed + next 6 h forecast covariates."""
    return _fetch_hourly(
        city,
        {
            "past_hours": config.WEATHER_PAST_HOURS,
            "forecast_hours": config.WEATHER_FORECAST_HOURS + 1,  # include hour t
        },
    )


def fetch_city_weather_archive(city: str, start_date: str, end_date: str) -> list[dict]:
    """Backfill pull from the ERA5 archive for [start_date, end_date] (YYYY-MM-DD).

    The archive is continuous back years — unlike the forecast endpoint's
    past_days, which only reaches ~2 weeks reliably (leaving deep-history
    gaps). All rows are historical, so is_forecast resolves to false.
    temperature_180m is absent from ERA5 and returns null (see FIELDS note).
    """
    return _fetch_hourly(
        city,
        {"start_date": start_date, "end_date": end_date},
        url=f"{config.open_meteo_archive_base()}/archive",
    )
