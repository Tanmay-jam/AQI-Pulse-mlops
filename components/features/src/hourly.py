"""Build aqi_hourly: raw readings -> rolling averages -> CPCB AQI.

Reads aqi_readings for a trailing window (plus the 24 h of history the
longest rolling average needs), aggregates to one concentration per
(station, parameter, hour), applies CPCB averaging windows, computes
sub-indices and the final AQI, and upserts one wide row per
(station, hour). Idempotent: recomputing an hour overwrites it with the
latest available data, so late-arriving readings self-heal on the next run.
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from features.src import config, cpcb

# Parameter -> (rolling window hours, min hours of data in the window).
WINDOWS = {
    "pm25": (24, config.MIN_HOURS_24H_WINDOW),
    "pm10": (24, config.MIN_HOURS_24H_WINDOW),
    "no2": (24, config.MIN_HOURS_24H_WINDOW),
    "so2": (24, config.MIN_HOURS_24H_WINDOW),
    "co": (8, config.MIN_HOURS_8H_WINDOW),
    "o3": (8, config.MIN_HOURS_8H_WINDOW),
}

AVG_COLUMN = {
    "pm25": "pm25_avg24",
    "pm10": "pm10_avg24",
    "no2": "no2_avg24",
    "so2": "so2_avg24",
    "co": "co_avg8",
    "o3": "o3_avg8",
}

UPSERT_COLUMNS = (
    ["station_id", "ts_hour"]
    + list(AVG_COLUMN.values())
    + [f"si_{p}" for p in AVG_COLUMN]
    + ["aqi", "dominant_pollutant", "n_readings"]
)


def _connect():
    return psycopg2.connect(**config.pg_params())


def _load_readings(since: dt.datetime) -> pd.DataFrame:
    sql = (
        "SELECT station_id, parameter, value, timestamp_utc "
        "FROM aqi_readings WHERE timestamp_utc >= %s"
    )
    with _connect() as conn:
        return pd.read_sql(sql, conn, params=(since,))


def _rolling_averages(readings: pd.DataFrame) -> pd.DataFrame:
    """One row per (station, hour) with each pollutant's windowed average
    and the count of raw readings behind that hour."""
    readings = readings.copy()
    readings["ts_hour"] = readings["timestamp_utc"].dt.floor("h")

    # Raw rows may be sub-hourly; first collapse to hourly concentrations.
    hourly = (
        readings.groupby(["station_id", "parameter", "ts_hour"])["value"]
        .agg(mean="mean", count="size")
        .reset_index()
    )

    frames = []
    for (station, parameter), grp in hourly.groupby(["station_id", "parameter"]):
        window, min_hours = WINDOWS[parameter]
        # Reindex to a continuous hourly range so gaps count as missing
        # hours (rolling over rows would silently span them).
        idx = pd.date_range(grp["ts_hour"].min(), grp["ts_hour"].max(), freq="h")
        series = grp.set_index("ts_hour")["mean"].reindex(idx)
        counts = grp.set_index("ts_hour")["count"].reindex(idx, fill_value=0)
        avg = series.rolling(window=window, min_periods=min_hours).mean()
        if parameter == "co":
            avg = avg / 1000.0  # µg/m³ -> mg/m³ (CPCB unit for CO)
        frames.append(
            pd.DataFrame(
                {
                    "station_id": station,
                    "ts_hour": idx,
                    "column": AVG_COLUMN[parameter],
                    "avg": avg.values,
                    "n_readings": counts.values,
                }
            )
        )
    if not frames:
        return pd.DataFrame()

    tall = pd.concat(frames, ignore_index=True)
    wide = tall.pivot_table(
        index=["station_id", "ts_hour"], columns="column", values="avg"
    ).reset_index()
    n = tall.groupby(["station_id", "ts_hour"])["n_readings"].sum().rename("n_readings")
    return wide.merge(n, on=["station_id", "ts_hour"])


def build(lookback_hours: int | None = None) -> int:
    """Recompute aqi_hourly for the trailing window. Returns rows upserted."""
    lookback = lookback_hours or config.LOOKBACK_HOURS
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(hours=lookback)
    history_start = window_start - dt.timedelta(hours=24)  # longest rolling window

    readings = _load_readings(history_start)
    if readings.empty:
        print("No readings in window — nothing to build.")
        return 0

    averages = _rolling_averages(readings)
    averages = averages[averages["ts_hour"] >= pd.Timestamp(window_start)]

    def _clean(v):  # NaN -> None so Postgres gets NULL, not 'NaN'
        return None if v is None or v != v else float(v)

    rows = []
    for rec in averages.to_dict("records"):
        sub_indices = {
            p: cpcb.sub_index(p, rec.get(AVG_COLUMN[p])) for p in AVG_COLUMN
        }
        result = cpcb.aqi(sub_indices)
        if result is None:  # CPCB reporting rule not met for this hour
            continue
        aqi_value, dominant = result
        rows.append(
            tuple(
                [rec["station_id"], rec["ts_hour"].to_pydatetime()]
                + [_clean(rec.get(col)) for col in AVG_COLUMN.values()]
                + [sub_indices[p] for p in AVG_COLUMN]
                + [aqi_value, dominant, int(rec["n_readings"])]
            )
        )
    if not rows:
        print("No station-hour met the CPCB reporting rule yet.")
        return 0

    update_cols = [c for c in UPSERT_COLUMNS if c not in ("station_id", "ts_hour")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO aqi_hourly ({', '.join(UPSERT_COLUMNS)}) VALUES %s "
        "ON CONFLICT (station_id, ts_hour) DO UPDATE SET "
        f"{set_clause}, computed_at = now()"
    )
    with _connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
    print(f"Upserted {len(rows)} aqi_hourly rows (window: last {lookback} h).")
    return len(rows)


if __name__ == "__main__":
    build()
