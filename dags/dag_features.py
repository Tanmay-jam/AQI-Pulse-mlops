"""DAG 2 — Features: materialize hourly AQI ground truth.

Runs at :15 past each hour (after dag_ingest, which runs at :00) and
rebuilds the trailing FEATURES_LOOKBACK_HOURS of aqi_hourly from raw
readings: rolling CPCB averages -> per-pollutant sub-indices -> AQI.

Why a fixed offset instead of an ExternalTaskSensor: the build is
idempotent over a trailing window, so even if one ingest run is late,
its data is simply picked up by the next hourly rebuild — a sensor
would add coupling without adding correctness.

aqi_hourly is the single AQI source for training labels, forecast input
lags, AND the ground truth the monitor DAG grades forecasts against —
computed once here so all three always agree.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "aqi-mlops",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="dag_features",
    schedule="15 * * * *",  # :15, after dag_ingest's top-of-hour run
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["aqi", "features"],
)
def dag_features():
    @task
    def build_aqi_hourly() -> int:
        from features.src import hourly

        return hourly.build()

    build_aqi_hourly()


dag_features()
