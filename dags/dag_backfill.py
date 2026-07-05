"""DAG 0 — Backfill (manual trigger only, schedule=None).

One-shot historical load: OpenAQ hourly aggregates + Open-Meteo archive
for the last `days_back` days (default 90, ceiling 92 from Open-Meteo's
past_days limit), then a full aqi_hourly rebuild over the same window.

Never runs on a schedule — trigger it from the UI ("Trigger DAG w/ config"
to override days_back) or CLI:

    airflow dags trigger dag_backfill --conf '{"days_back": 90}'

Safe to re-run: backfill ends where live coverage begins (cutoffs derived
from the oldest live rows), and all writes land on natural keys, so a
repeat run inserts nothing new. Backfilled data flows through the same
normalize/validate path as live data — same unit fixes, same rules.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "aqi-mlops",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="dag_backfill",
    schedule=None,          # manual trigger only — this is a tool, not a job
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"days_back": 90},
    default_args=DEFAULT_ARGS,
    tags=["aqi", "backfill"],
)
def dag_backfill():
    @task
    def backfill_openaq(**context) -> dict:
        from ingest.src import backfill

        return backfill.backfill_readings(int(context["params"]["days_back"]))

    @task
    def backfill_weather(**context) -> dict:
        from ingest.src import backfill

        return backfill.backfill_weather(int(context["params"]["days_back"]))

    @task
    def rebuild_aqi_hourly(**context) -> int:
        from features.src import hourly

        days = int(context["params"]["days_back"])
        return hourly.build(lookback_hours=days * 24 + 24)

    # Both sources land first (parallel), then AQI materializes over the
    # whole backfilled window in one idempotent rebuild.
    [backfill_openaq(), backfill_weather()] >> rebuild_aqi_hourly()


dag_backfill()
