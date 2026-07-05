"""Standalone entrypoint for the ingest component.

The Airflow DAG calls the individual functions (health/pull/validate/load)
as separate tasks. This `run()` does the whole thing in one process — handy
for a manual test (`python -m ingest.src.main`) or a future DockerOperator.
"""
from __future__ import annotations

import datetime as dt
import pathlib

from ingest.src import config, meteo_client, openaq_client, validator, writer


def run() -> int:
    total = 0
    now = dt.datetime.now(dt.timezone.utc)
    writer.upsert_cities(config.CITIES)
    for city in config.CITIES:
        records = openaq_client.collect_city_records(city)
        raw_path = (
            pathlib.Path(config.data_root())
            / "raw"
            / city.lower()
            / now.strftime("%Y-%m-%d")
            / now.strftime("%H")
            / "measurements.json"
        )
        writer.write_raw_json(records, raw_path)
        clean = validator.validate(records)
        writer.upsert_stations(clean)
        inserted = writer.upsert_readings(clean)

        weather = meteo_client.fetch_city_weather(city)
        weather_rows = writer.upsert_weather(weather)

        total += inserted
        print(
            f"[{city}] pulled={len(records)} valid={len(clean)} "
            f"inserted={inserted} weather_rows={weather_rows}"
        )
    return total


if __name__ == "__main__":
    run()
