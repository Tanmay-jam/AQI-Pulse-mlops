"""Configuration for the features component.

Deliberately self-contained (its own pg_params rather than importing from
the ingest package) so it can be packaged into its own Docker image later,
per the one-component-one-container design.
"""
from __future__ import annotations

import os

# How far back each run recomputes aqi_hourly. Recomputing a trailing
# window (idempotent upsert) picks up late-arriving readings without
# needing cross-DAG sensors.
LOOKBACK_HOURS: int = int(os.getenv("FEATURES_LOOKBACK_HOURS", "30"))

# Minimum distinct hours of data required inside a rolling window before
# we trust its average. Kept low for the cold-start weeks (the pipeline
# ingests `latest` only, so history accumulates one hour at a time);
# tighten toward CPCB's 16-of-24 once backfill exists.
MIN_HOURS_24H_WINDOW: int = int(os.getenv("FEATURES_MIN_HOURS_24", "6"))
MIN_HOURS_8H_WINDOW: int = int(os.getenv("FEATURES_MIN_HOURS_8", "2"))


def pg_params() -> dict:
    """psycopg2 connection kwargs for the application database."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "aqi"),
        "user": os.getenv("POSTGRES_USER", "aqi"),
        "password": os.getenv("POSTGRES_PASSWORD", "aqi"),
    }
