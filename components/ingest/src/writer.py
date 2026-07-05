"""Persistence: raw JSON to the data lake + idempotent upserts into Postgres.

All DDL lives in infra/postgres/schema.sql (applied on first Postgres
init) — this module assumes the tables exist and fails loudly if not.
"""
from __future__ import annotations

import json
import pathlib

import psycopg2
from psycopg2.extras import execute_values

from ingest.src import config

READING_COLUMNS = [
    "station_id",
    "city",
    "parameter",
    "value",
    "unit",
    "timestamp_utc",
]

WEATHER_COLUMNS = [
    "city",
    "ts_hour",
    "temperature_c",
    "temperature_180m_c",
    "relative_humidity",
    "dew_point_c",
    "wind_speed_ms",
    "wind_direction_deg",
    "wind_gusts_ms",
    "boundary_layer_height_m",
    "surface_pressure_hpa",
    "cloud_cover_pct",
    "shortwave_radiation_wm2",
    "precipitation_mm",
    "is_forecast",
]


def _connect():
    return psycopg2.connect(**config.pg_params())


def write_raw_json(records: list[dict], path: str | pathlib.Path) -> str:
    """Write the raw pull to the data lake (local dir now, GCS later)."""
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(records, indent=2, default=str))
    return str(p)


def upsert_readings(records: list[dict]) -> int:
    """Insert new readings, skipping any that already exist (natural key
    station/parameter/timestamp makes reruns idempotent). Returns rows added."""
    if not records:
        return 0
    rows = [tuple(rec.get(col) for col in READING_COLUMNS) for rec in records]
    sql = (
        f"INSERT INTO aqi_readings ({', '.join(READING_COLUMNS)}) VALUES %s "
        "ON CONFLICT (station_id, parameter, timestamp_utc) DO NOTHING"
    )
    with _connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        return cur.rowcount


def upsert_stations(records: list[dict]) -> int:
    """Register the stations seen in a batch of readings.

    New stations are inserted; known ones just get last_seen bumped (name
    and coordinates refreshed in case OpenAQ metadata changed).
    """
    seen: dict[int, tuple] = {}
    for rec in records:
        seen[rec["station_id"]] = (
            rec["station_id"],
            rec.get("station_name"),
            rec["city"],
            rec.get("latitude"),
            rec.get("longitude"),
        )
    if not seen:
        return 0
    sql = (
        "INSERT INTO stations (station_id, name, city, latitude, longitude) VALUES %s "
        "ON CONFLICT (station_id) DO UPDATE SET "
        "name = EXCLUDED.name, latitude = EXCLUDED.latitude, "
        "longitude = EXCLUDED.longitude, last_seen = now()"
    )
    with _connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, list(seen.values()))
        return len(seen)


def upsert_cities(cities_cfg: dict) -> int:
    """Register configured cities (name, weather centroid, bbox) so spatial
    features can read the centroid from the DB instead of Python config."""
    rows = [
        (name, cfg["lat"], cfg["lon"], cfg.get("bbox"))
        for name, cfg in cities_cfg.items()
    ]
    if not rows:
        return 0
    sql = (
        "INSERT INTO cities (city, centroid_lat, centroid_lon, bbox) VALUES %s "
        "ON CONFLICT (city) DO UPDATE SET "
        "centroid_lat = EXCLUDED.centroid_lat, centroid_lon = EXCLUDED.centroid_lon, "
        "bbox = EXCLUDED.bbox, updated_at = now()"
    )
    with _connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        return len(rows)


def upsert_weather(records: list[dict]) -> int:
    """Upsert city-hour weather rows.

    Observed data always wins; a forecast never downgrades an observed row.
    The guard allows the write when the existing row is a forecast (any newer
    value replaces it) OR the incoming row is observed (observed refreshes
    observed too — this is what lets an archive backfill correct the
    null/partial rows an earlier forecast-based pull left behind). The only
    blocked case is observed-existing ← forecast-incoming.
    """
    if not records:
        return 0
    rows = [tuple(rec.get(col) for col in WEATHER_COLUMNS) for rec in records]
    set_clause = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in WEATHER_COLUMNS if col not in ("city", "ts_hour")
    )
    sql = (
        f"INSERT INTO weather_hourly ({', '.join(WEATHER_COLUMNS)}) VALUES %s "
        "ON CONFLICT (city, ts_hour) DO UPDATE SET "
        f"{set_clause}, updated_at = now() "
        "WHERE weather_hourly.is_forecast OR NOT EXCLUDED.is_forecast"
    )
    with _connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        return cur.rowcount
