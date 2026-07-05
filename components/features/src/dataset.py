"""Analytical dataset builder — the drift-free join + supervised transforms.

This module is the single source of truth for how the five tables become a
model-ready frame, shared by exploratory notebooks and (later) the training
DAG, so a feature never means one thing in a prototype and another in
production.

Two layers:
  load_base_frame()  -> one row per (station, hour): AQI + sub-indices +
                        weather + station/city metadata + physics features.
                        This is what gets exported as the Colab snapshot;
                        it holds NO lags or targets, so EDA stays free to
                        decide those.
  make_supervised()  -> adds per-station lag/rolling features, calendar
                        features, and t+1..t+6 AQI targets on a gap-safe
                        hourly index. Call this AFTER EDA informs the lags.

Run `python -m features.src.dataset` (inside the airflow container, where
PYTHONPATH and the DB host are set) to write a Parquet snapshot.
"""
from __future__ import annotations

import datetime as dt
import os
import pathlib

import numpy as np
import pandas as pd
import psycopg2

from features.src import config

# ─────────────────────────────────────────────────────────────
# Layer 1 — the base analytical frame (join only)
# ─────────────────────────────────────────────────────────────

BASE_QUERY = """
SELECT h.station_id, h.ts_hour, h.aqi, h.dominant_pollutant, h.n_readings,
       h.pm25_avg24, h.pm10_avg24, h.no2_avg24, h.so2_avg24, h.co_avg8, h.o3_avg8,
       h.si_pm25, h.si_pm10, h.si_no2, h.si_so2, h.si_co, h.si_o3,
       s.name AS station_name, s.city, s.latitude, s.longitude,
       c.centroid_lat, c.centroid_lon,
       w.temperature_c, w.temperature_180m_c, w.relative_humidity, w.dew_point_c,
       w.wind_speed_ms, w.wind_direction_deg, w.wind_gusts_ms,
       w.boundary_layer_height_m, w.surface_pressure_hpa, w.cloud_cover_pct,
       w.shortwave_radiation_wm2, w.precipitation_mm,
       w.is_forecast AS weather_is_forecast
FROM aqi_hourly h
JOIN stations s USING (station_id)
LEFT JOIN cities c ON c.city = s.city
LEFT JOIN weather_hourly w ON w.city = s.city AND w.ts_hour = h.ts_hour
ORDER BY h.station_id, h.ts_hour
"""


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km (vectorised over pandas Series)."""
    r = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def _add_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Physics + spatial features that are pure functions of stored columns.

    inversion_strength : T(180 m) − T(2 m); positive = warm lid aloft trapping
                         pollution near the ground.
    ventilation_coef   : boundary-layer height × wind speed; the classic
                         dispersion capacity index (bigger = cleaner air).
    dist_to_centroid_km: station distance from the city weather point — lets a
                         model weight how representative the shared weather is.
    """
    df["inversion_strength"] = df["temperature_180m_c"] - df["temperature_c"]
    df["ventilation_coef"] = df["boundary_layer_height_m"] * df["wind_speed_ms"]
    df["dist_to_centroid_km"] = _haversine_km(
        df["latitude"], df["longitude"], df["centroid_lat"], df["centroid_lon"]
    )
    return df


def load_base_frame(conn=None) -> pd.DataFrame:
    """Return the joined station-hour base frame (no lags/targets)."""
    own = conn is None
    if own:
        conn = psycopg2.connect(**config.pg_params())
    try:
        df = pd.read_sql(BASE_QUERY, conn, parse_dates=["ts_hour"])
    finally:
        if own:
            conn.close()
    return _add_derived(df)


# ─────────────────────────────────────────────────────────────
# Layer 2 — supervised transform (lags + targets), gap-safe
# ─────────────────────────────────────────────────────────────

LAG_HOURS = (1, 2, 3, 6, 12, 24)
ROLL_WINDOWS = (3, 6, 12, 24)
POLLUTANT_LAG1 = ("pm25_avg24", "pm10_avg24", "no2_avg24")
HORIZONS = (1, 2, 3, 4, 5, 6)


def add_calendar(df: pd.DataFrame, ts_col: str = "ts_hour") -> pd.DataFrame:
    ts = df[ts_col]
    df["hour"] = ts.dt.hour
    df["dow"] = ts.dt.dayofweek
    df["month"] = ts.dt.month
    df["is_weekend"] = (ts.dt.dayofweek >= 5).astype(int)
    return df


def make_supervised(
    base: pd.DataFrame,
    lag_hours=LAG_HOURS,
    roll_windows=ROLL_WINDOWS,
    horizons=HORIZONS,
) -> pd.DataFrame:
    """Build lag features and t+h AQI targets, per station, on a *complete*
    hourly index so a "lag 1" is genuinely one hour back — not merely the
    previous existing row across a data gap.

    Rows where any requested lag or target is missing are dropped, so the
    result is directly trainable. Weather-at-t columns pass through as the
    covariates known at issue time.
    """
    out_frames = []
    for sid, g in base.groupby("station_id"):
        g = g.sort_values("ts_hour").set_index("ts_hour")
        full = pd.date_range(g.index.min(), g.index.max(), freq="h")
        g = g.reindex(full)
        g["station_id"] = sid

        for lag in lag_hours:
            g[f"aqi_lag{lag}"] = g["aqi"].shift(lag)
        for win in roll_windows:
            g[f"aqi_roll{win}_mean"] = g["aqi"].shift(1).rolling(win).mean()
            g[f"aqi_roll{win}_std"] = g["aqi"].shift(1).rolling(win).std()
        for col in POLLUTANT_LAG1:
            g[f"{col}_lag1"] = g[col].shift(1)
        for h in horizons:
            g[f"aqi_t+{h}"] = g["aqi"].shift(-h)

        g = g.rename_axis("ts_hour").reset_index()
        out_frames.append(g)

    out = pd.concat(out_frames, ignore_index=True)
    out = add_calendar(out)
    target_cols = [f"aqi_t+{h}" for h in horizons]
    lag_cols = [f"aqi_lag{lag}" for lag in lag_hours]
    return out.dropna(subset=target_cols + lag_cols).reset_index(drop=True)


def time_split(df: pd.DataFrame, holdout_days: int = 7):
    """Global time-based split (never random): the last `holdout_days` across
    all stations are the test set, everything earlier is train."""
    cutoff = df["ts_hour"].max() - pd.Timedelta(days=holdout_days)
    return df[df["ts_hour"] <= cutoff].copy(), df[df["ts_hour"] > cutoff].copy()


# ─────────────────────────────────────────────────────────────
# Snapshot export (for Colab / offline prototyping)
# ─────────────────────────────────────────────────────────────

def export_snapshot(out_dir: str | None = None) -> str:
    """Write the base frame to Parquet and print a summary. Returns the path."""
    df = load_base_frame()
    root = out_dir or os.getenv("DATA_ROOT", "/opt/airflow/data")
    snap_dir = pathlib.Path(root) / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)

    span = f"{df['ts_hour'].min():%Y%m%d}_{df['ts_hour'].max():%Y%m%d}"
    path = snap_dir / f"aqi_base_{span}.parquet"
    df.to_parquet(path, index=False)

    size_mb = path.stat().st_size / 1e6
    print(f"Wrote {path}  ({len(df):,} rows × {df.shape[1]} cols, {size_mb:.2f} MB)")
    print(f"Span : {df['ts_hour'].min()} → {df['ts_hour'].max()}")
    print(f"Stations: {df['station_id'].nunique()}  |  Cities: {df['city'].nunique()}")
    weather_missing = df['temperature_c'].isna().mean()
    print(f"Rows with no weather match: {weather_missing:.1%}")
    print("Null fraction by column (top 8):")
    print((df.isna().mean().sort_values(ascending=False).head(8)).to_string())
    return str(path)


if __name__ == "__main__":
    export_snapshot()
