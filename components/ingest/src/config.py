"""Central configuration for the ingest component.

Values that differ between local and cloud (DB host, data root, API base/key)
are read from environment variables so the same code runs unchanged in Week 3.
"""
from __future__ import annotations

import os

# Pollutants we keep. OpenAQ exposes many parameters; we ingest only these.
ALLOWED_PARAMETERS: set[str] = {"pm25", "pm10", "no2", "co", "so2", "o3"}

# Per-city config. `bbox` ("minLon,minLat,maxLon,maxLat", OpenAQ v3 format)
# selects the stations; `lat`/`lon` (the bbox centroid) is the representative
# point for city-level Open-Meteo weather — one weather row per (city, hour)
# serves every station in the city (stations join weather via their `city`).
# Week 1 is Delhi-only; adding a city later is just another entry here.
CITIES: dict[str, dict] = {
    "Delhi": {"bbox": "76.84,28.40,77.35,28.88", "lat": 28.64, "lon": 77.095},
}

# Hours of weather to pull around "now": past hours refresh forecast rows
# with observed values; future hours are the t+1..t+6 forecast covariates.
WEATHER_PAST_HOURS: int = 24
WEATHER_FORECAST_HOURS: int = 6

# Cap stations per run so we stay well under the 60 req/min free-tier rate limit
# while proving the pipeline. Raise/remove once on the cloud.
MAX_LOCATIONS: int = int(os.getenv("INGEST_MAX_LOCATIONS", "15"))

REQUEST_TIMEOUT: int = int(os.getenv("INGEST_REQUEST_TIMEOUT", "30"))

# OpenAQ's /locations directory still lists dead stations, and /latest for a
# dead sensor doesn't error — it echoes the sensor's last-ever reading
# (observed: values from 2016). The live ingest drops anything older than
# this so a dead sensor can never leak years-old values into aqi_readings.
# Applies to the live /latest path only; the backfill path is *supposed* to
# ingest old timestamps and bypasses this filter.
MAX_READING_AGE_HOURS: int = int(os.getenv("INGEST_MAX_READING_AGE_HOURS", "3"))

# Backfill: how far back the manual dag_backfill reaches by default.
BACKFILL_DAYS: int = int(os.getenv("BACKFILL_DAYS", "90"))

# Boundary (hours before now) between backfilled history and the live-owned
# recent window. The live DAG pulls the trailing 24 h hourly, so it reliably
# owns the last day; backfill fills strictly before this line.
BACKFILL_HANDOFF_HOURS: int = int(os.getenv("BACKFILL_HANDOFF_HOURS", "24"))

# Seconds between OpenAQ requests during backfill pagination, to stay under
# the free tier's 60 req/min.
OPENAQ_REQUEST_INTERVAL: float = float(os.getenv("OPENAQ_REQUEST_INTERVAL", "1.1"))


def data_root() -> str:
    """Local stand-in for the GCS data lake (swapped for gs:// in Week 3)."""
    return os.getenv("DATA_ROOT", "/opt/airflow/data")


def openaq_base() -> str:
    return os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3").rstrip("/")


def openaq_key() -> str:
    return os.getenv("OPENAQ_API_KEY", "")


def open_meteo_base() -> str:
    """Live/forecast endpoint (recent past + forecast). No API key needed."""
    return os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1").rstrip("/")


def open_meteo_archive_base() -> str:
    """Archive endpoint (ERA5 reanalysis) for deep history. The /forecast
    endpoint's past_days only reaches back reliably ~2 weeks; the archive
    serves the full continuous history and is the correct backfill source."""
    return os.getenv(
        "OPEN_METEO_ARCHIVE_URL", "https://archive-api.open-meteo.com/v1"
    ).rstrip("/")


def pg_params() -> dict:
    """psycopg2 connection kwargs for the application database."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "aqi"),
        "user": os.getenv("POSTGRES_USER", "aqi"),
        "password": os.getenv("POSTGRES_PASSWORD", "aqi"),
    }
