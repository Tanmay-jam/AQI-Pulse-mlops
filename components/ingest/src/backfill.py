"""Historical backfill: OpenAQ hourly aggregates + Open-Meteo archive.

One-shot (manually triggered) load of the last N days into the same raw
tables the live DAG feeds — through the same normalize/validate/upsert
path, so backfilled data obeys the same rules as live data.

Overlap protection: live ingestion has been accumulating rows since the
stack first came up. Backfill must not double-cover those hours (the
features builder averages whatever falls in an hour, so a backfilled
hourly aggregate PLUS a live instantaneous reading in the same hour would
skew the mean). Each backfill therefore ends at a cutoff derived from the
oldest *live-era* row already in the table, floored to the hour:

    readings: min(timestamp_utc) newer than now - 2*BACKFILL_DAYS
    weather:  min(ts_hour)

Hours >= cutoff belong to live ingestion; backfill fills strictly before.
Re-running is safe: the same window re-lands on the same natural keys
(ON CONFLICT DO NOTHING / observed-wins upsert).
"""
from __future__ import annotations

import datetime as dt
import pathlib

import psycopg2

from ingest.src import config, meteo_client, openaq_client, validator, writer


def _floor_hour(ts: dt.datetime) -> dt.datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def _one_value(sql: str):
    with psycopg2.connect(**config.pg_params()) as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


def handoff() -> dt.datetime:
    """Boundary between backfill (older) and live (recent) coverage.

    A FIXED offset from now, not a value derived from the tables — because
    backfill itself extends min(ts) far into the past, so a data-derived
    cutoff would march the window backwards on every re-run. The live DAG
    pulls the trailing 24 h each hour, so it reliably owns [handoff, now];
    backfill fills strictly before it. This makes re-runs stable and
    idempotent.
    """
    return _floor_hour(dt.datetime.now(dt.timezone.utc)) - dt.timedelta(
        hours=config.BACKFILL_HANDOFF_HOURS
    )


def backfill_readings(days_back: int | None = None) -> dict:
    """Pull OpenAQ hourly history for every city up to the live handoff."""
    days = days_back or config.BACKFILL_DAYS
    end = handoff()
    start = end - dt.timedelta(days=days)
    counts: dict[str, int] = {}
    for city in config.CITIES:
        records = openaq_client.collect_city_history(
            city, start.isoformat(), end.isoformat()
        )
        raw_path = (
            pathlib.Path(config.data_root())
            / "raw"
            / city.lower()
            / "backfill"
            / f"{start:%Y-%m-%d}_{end:%Y-%m-%d}.json"
        )
        writer.write_raw_json(records, raw_path)
        clean = validator.validate(records)
        writer.upsert_stations(clean)
        inserted = writer.upsert_readings(clean)
        counts[city] = inserted
        print(
            f"[{city}] backfill {start:%Y-%m-%d} -> {end:%Y-%m-%d %H:%M}: "
            f"pulled={len(records)} valid={len(clean)} inserted={inserted}"
        )
    return counts


def backfill_weather(days_back: int | None = None) -> dict:
    """Pull ERA5 archive weather for every city up to the live weather cutoff.

    Uses the archive endpoint (continuous history) rather than the forecast
    endpoint's past_days (which only reaches ~2 weeks). Rows at/after the
    live cutoff are excluded so archive and live never double-cover an hour.
    """
    days = days_back or config.BACKFILL_DAYS
    cutoff = handoff()
    start = (cutoff - dt.timedelta(days=days)).date().isoformat()
    end = cutoff.date().isoformat()
    counts: dict[str, int] = {}
    for city in config.CITIES:
        records = meteo_client.fetch_city_weather_archive(city, start, end)
        before_cutoff = [
            r for r in records
            if dt.datetime.fromisoformat(r["ts_hour"]) < cutoff
        ]
        inserted = writer.upsert_weather(before_cutoff)
        counts[city] = inserted
        print(
            f"[{city}] weather archive backfill {start}..{end}: "
            f"pulled={len(records)} kept<{cutoff:%Y-%m-%d %H:%M}={len(before_cutoff)} "
            f"upserted={inserted}"
        )
    return counts
