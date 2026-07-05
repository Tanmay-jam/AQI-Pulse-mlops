"""Thin client over the OpenAQ v3 API.

Live flow for one city (hourly DAG):
  1. fetch_locations(bbox)          -> stations in the city, each with its sensors
  2. for each station: fetch_latest -> most recent value per sensor
  3. join sensor -> parameter, drop stale echoes, dedupe twin sensors

Backfill flow (manual DAG): per sensor, page through /sensors/{id}/hours
for a date range — hourly aggregates, one value per station-parameter-hour.

Auth is via the `X-API-Key` header (free key from openaq.org).
"""
from __future__ import annotations

import datetime as dt
import time

import requests

from ingest.src import config


def _headers() -> dict:
    return {"X-API-Key": config.openaq_key()}


def health() -> bool:
    """Cheap liveness probe used by the DAG's first task."""
    resp = requests.get(
        f"{config.openaq_base()}/locations",
        params={"limit": 1},
        headers=_headers(),
        timeout=config.REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return True


def fetch_locations(bbox: str) -> list[dict]:
    """Stations within a bounding box. Each result carries a `sensors` list
    that maps sensor id -> parameter (name + units)."""
    resp = requests.get(
        f"{config.openaq_base()}/locations",
        params={"bbox": bbox, "limit": 200},
        headers=_headers(),
        timeout=config.REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def fetch_latest(location_id: int) -> list[dict]:
    """Latest measurement per sensor for one station."""
    resp = requests.get(
        f"{config.openaq_base()}/locations/{location_id}/latest",
        headers=_headers(),
        timeout=config.REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def sensor_map(location: dict) -> dict[int, dict]:
    """Build {sensor_id: {parameter, unit}} from a location's sensors list."""
    mapping: dict[int, dict] = {}
    for sensor in location.get("sensors") or []:
        param = sensor.get("parameter") or {}
        mapping[sensor.get("id")] = {
            "parameter": param.get("name"),
            "unit": param.get("units"),
        }
    return mapping


def _parse_utc(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def drop_stale(records: list[dict], max_age_hours: int | None = None) -> list[dict]:
    """Drop records older than the freshness window (live path only).

    Dead stations stay listed in /locations and their /latest echoes the
    sensor's last-ever reading instead of erroring — without this filter
    those zombie values (observed: 2016) would re-insert every hour.
    """
    max_age = max_age_hours or config.MAX_READING_AGE_HOURS
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max_age)
    return [r for r in records if (ts := _parse_utc(r.get("timestamp_utc"))) and ts >= cutoff]


def dedupe_twin_sensors(records: list[dict]) -> list[dict]:
    """Keep one record per (station, parameter): the newest timestamp.

    CPCB stations expose twin sensors per pollutant (a legacy µg/m³ one and
    a current one). Twins share our primary key, so without this the insert
    order would decide which survives — non-deterministically.
    """
    best: dict[tuple, tuple] = {}
    for rec in records:
        key = (rec.get("station_id"), rec.get("parameter"))
        ts = _parse_utc(rec.get("timestamp_utc"))
        kept = best.get(key)
        if kept is None or (ts and (kept[0] is None or ts > kept[0])):
            best[key] = (ts, rec)
    return [rec for _, rec in best.values()]


def collect_city_records(city: str) -> list[dict]:
    """Return a flat list of *fresh, deduplicated* measurement records for `city`.

    Each record: station_id, station_name, city, parameter, value, unit,
    latitude, longitude, timestamp_utc. (An OpenAQ "location" is our
    "station"; lat/lon/name feed the stations table, not aqi_readings.)
    """
    bbox = config.CITIES[city]["bbox"]
    locations = fetch_locations(bbox)[: config.MAX_LOCATIONS]

    records: list[dict] = []
    for loc in locations:
        smap = sensor_map(loc)
        loc_coords = loc.get("coordinates") or {}
        for latest in fetch_latest(loc.get("id")):
            meta = smap.get(latest.get("sensorsId"))
            if not meta or meta["parameter"] not in config.ALLOWED_PARAMETERS:
                continue
            coords = latest.get("coordinates") or loc_coords
            records.append(
                {
                    "station_id": loc.get("id"),
                    "station_name": loc.get("name"),
                    "city": city,
                    "parameter": meta["parameter"],
                    "value": latest.get("value"),
                    "unit": meta["unit"],
                    "latitude": coords.get("latitude"),
                    "longitude": coords.get("longitude"),
                    "timestamp_utc": (latest.get("datetime") or {}).get("utc"),
                }
            )
    return dedupe_twin_sensors(drop_stale(records))


def fetch_sensor_hours(
    sensor_id: int, datetime_from: str, datetime_to: str, page: int = 1
) -> list[dict]:
    """One page of hourly aggregates for a sensor (max 1000 rows/page)."""
    resp = requests.get(
        f"{config.openaq_base()}/sensors/{sensor_id}/hours",
        params={
            "datetime_from": datetime_from,
            "datetime_to": datetime_to,
            "limit": 1000,
            "page": page,
        },
        headers=_headers(),
        timeout=config.REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def collect_city_history(city: str, datetime_from: str, datetime_to: str) -> list[dict]:
    """Hourly-aggregate records for every allowed-parameter sensor in `city`.

    Same record shape as collect_city_records. timestamp_utc is the hour
    bucket's start (period.datetimeFrom) — CPCB buckets are IST-aligned so
    these land at :30 UTC; the features builder floors them to the UTC hour.
    Dead sensors simply return zero rows for a recent range, so no staleness
    filter is needed here. Paced to respect the 60 req/min free tier.
    """
    bbox = config.CITIES[city]["bbox"]
    locations = fetch_locations(bbox)[: config.MAX_LOCATIONS]

    records: list[dict] = []
    for loc in locations:
        smap = sensor_map(loc)
        loc_coords = loc.get("coordinates") or {}
        for sensor_id, meta in smap.items():
            if meta["parameter"] not in config.ALLOWED_PARAMETERS:
                continue
            page = 1
            while True:
                time.sleep(config.OPENAQ_REQUEST_INTERVAL)
                results = fetch_sensor_hours(sensor_id, datetime_from, datetime_to, page)
                for item in results:
                    period = item.get("period") or {}
                    param = item.get("parameter") or {}
                    records.append(
                        {
                            "station_id": loc.get("id"),
                            "station_name": loc.get("name"),
                            "city": city,
                            "parameter": param.get("name") or meta["parameter"],
                            "value": item.get("value"),
                            "unit": param.get("units") or meta["unit"],
                            "latitude": loc_coords.get("latitude"),
                            "longitude": loc_coords.get("longitude"),
                            "timestamp_utc": (period.get("datetimeFrom") or {}).get("utc"),
                        }
                    )
                if len(results) < 1000:
                    break
                page += 1
    return records
