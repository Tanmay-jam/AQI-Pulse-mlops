"""DAG 1 — Ingest.

Hourly: check the OpenAQ API is up, pull Delhi pollutant measurements,
validate them, and upsert into Postgres (stations + aqi_readings). A
parallel branch pulls city-level weather from Open-Meteo (past 24 h
observed + next 6 h forecast covariates) into weather_hourly.

Both sources are stored in UTC and keyed to the hour, so pollutant and
weather rows join on (city, ts_hour) with no timezone ambiguity.

Written with the TaskFlow API so each step is a visible task in the
Airflow graph and data passes between them via XCom. The task functions
import the ingest package lazily (inside each task) so a missing
dependency can never break DAG *parsing* — only the run.
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
    dag_id="dag_ingest",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,          # Week 1: no historical backfill (avoid a run flood)
    max_active_runs=1,      # never overlap ingest runs
    default_args=DEFAULT_ARGS,
    tags=["aqi", "ingest"],
)
def dag_ingest():
    @task
    def check_api_health() -> None:
        from ingest.src import openaq_client

        openaq_client.health()
        print("OpenAQ API healthy.")

    @task
    def pull_measurements(**context) -> list[dict]:
        import pathlib

        from ingest.src import config, openaq_client, writer

        city = "Delhi"
        records = openaq_client.collect_city_records(city)

        start = context["data_interval_start"]
        raw_path = (
            pathlib.Path(config.data_root())
            / "raw"
            / city.lower()
            / start.strftime("%Y-%m-%d")
            / start.strftime("%H")
            / "measurements.json"
        )
        writer.write_raw_json(records, raw_path)
        print(f"Pulled {len(records)} records for {city}; raw -> {raw_path}")
        return records

    @task
    def validate_schema(records: list[dict]) -> list[dict]:
        from ingest.src import validator

        clean = validator.validate(records)
        print(f"Validated {len(clean)} records.")
        return clean

    @task
    def insert_postgres(records: list[dict]) -> int:
        from ingest.src import config, writer

        writer.upsert_cities(config.CITIES)
        n_stations = writer.upsert_stations(records)
        inserted = writer.upsert_readings(records)
        print(
            f"Registered {n_stations} stations; inserted {inserted} new "
            "readings (duplicates skipped via ON CONFLICT)."
        )
        return inserted

    @task
    def pull_weather() -> int:
        """Open-Meteo weather for each city: observed rows overwrite earlier
        forecast rows for the same hour; observed rows are never downgraded."""
        from ingest.src import config, meteo_client, writer

        total = 0
        for city in config.CITIES:
            records = meteo_client.fetch_city_weather(city)
            total += writer.upsert_weather(records)
            print(f"[{city}] upserted weather rows: {len(records)} pulled")
        return total

    healthy = check_api_health()
    raw = pull_measurements()
    healthy >> raw  # pull only after the API is confirmed up
    insert_postgres(validate_schema(raw))
    pull_weather()  # independent branch — Open-Meteo needs no health gate


dag_ingest()
