"""Pandera validation — the gate between the API and our database.

Bad data fails the DAG loudly rather than being silently written to
Postgres. Order matters: units are normalized to µg/m³ *first* (records
with unconvertible units are dropped), then row-level junk is dropped
(nulls; negative concentrations — CPCB instrument sentinels/noise), then
the schema asserts parameter in the allowed set, unit canonical, timestamp
parseable. Batch-level corruption is caught by the unusable-fraction
threshold: junk rows are expected one by one, not by the fifth.
"""
from __future__ import annotations

import pandas as pd
import pandera as pa

from ingest.src import config, units

# If more than this fraction of a batch is unusable (null value/timestamp,
# or dropped in unit conversion), the whole batch is rejected — something
# is wrong upstream, don't half-ingest.
MAX_NULL_FRACTION = 0.2

SCHEMA = pa.DataFrameSchema(
    {
        "station_id": pa.Column(int, coerce=True),
        "station_name": pa.Column(str, nullable=True),
        "city": pa.Column(str),
        "parameter": pa.Column(str, pa.Check.isin(sorted(config.ALLOWED_PARAMETERS))),
        "value": pa.Column(float, pa.Check.ge(0), coerce=True, nullable=False),
        "unit": pa.Column(str, pa.Check.eq(units.CANONICAL_UNIT)),
        "latitude": pa.Column(float, nullable=True, coerce=True),
        "longitude": pa.Column(float, nullable=True, coerce=True),
        "timestamp_utc": pa.Column("datetime64[ns, UTC]"),
    },
    strict=False,
)


def _to_frame(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    return df


def validate(records: list[dict]) -> list[dict]:
    """Normalize units, validate, and return cleaned records
    (JSON/XCom-serializable).

    Raises ValueError on an empty or too-lossy batch, and
    pandera.SchemaError if any surviving row breaks a rule
    (e.g. negative value, bad parameter).
    """
    if not records:
        raise ValueError("No records to validate — ingest returned an empty batch.")

    n_total = len(records)
    df = _to_frame(units.normalize(records))
    if not df.empty:
        df = df.dropna(subset=["value", "timestamp_utc"])
        # CPCB instruments emit negative sentinels (-999, -10000) during
        # malfunction/calibration windows, plus small negative noise around
        # zero — routine in historical data. Row-level junk, not batch-fatal:
        # drop and count toward the unusable fraction below.
        df = df[df["value"] >= 0]
    unusable_fraction = 1 - len(df) / n_total
    if unusable_fraction > MAX_NULL_FRACTION:
        raise ValueError(
            f"Unusable fraction {unusable_fraction:.0%} (nulls + negatives + "
            f"unconvertible units) exceeds limit {MAX_NULL_FRACTION:.0%}"
        )

    validated = SCHEMA.validate(df).copy()
    # ISO strings so the result serializes cleanly through Airflow XCom.
    validated["timestamp_utc"] = validated["timestamp_utc"].apply(lambda t: t.isoformat())
    return validated.to_dict("records")
