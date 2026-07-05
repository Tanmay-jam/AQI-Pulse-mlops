"""Unit tests for the ingest validator. No network or database needed."""
import pandera as pa
import pytest

from ingest.src.validator import validate


def _record(**overrides) -> dict:
    base = {
        "station_id": 1,
        "station_name": "Delhi - Anand Vihar",
        "city": "Delhi",
        "parameter": "pm25",
        "value": 42.0,
        "unit": "µg/m³",
        "latitude": 28.65,
        "longitude": 77.31,
        "timestamp_utc": "2026-07-02T06:00:00Z",
    }
    base.update(overrides)
    return base


def test_valid_batch_passes():
    out = validate([_record(), _record(parameter="no2", value=12.5)])
    assert len(out) == 2
    assert out[0]["parameter"] == "pm25"
    # timestamp comes back as an ISO string (XCom-serializable)
    assert isinstance(out[0]["timestamp_utc"], str)


def test_gas_units_normalized_to_ugm3():
    # 10 ppb NO2 = 10 * 46.01 / 24.45 ≈ 18.82 µg/m³
    out = validate([_record(), _record(parameter="no2", value=10.0, unit="ppb")])
    no2 = next(r for r in out if r["parameter"] == "no2")
    assert no2["unit"] == "µg/m³"
    assert no2["value"] == pytest.approx(18.82, abs=0.01)


def test_negative_values_dropped_as_row_junk():
    # CPCB sentinels (-999/-10000) and noise are row-level junk, not
    # batch-fatal — the batch passes with the bad row removed...
    out = validate([_record()] * 8 + [_record(value=-999.0)])
    assert len(out) == 8
    assert all(r["value"] >= 0 for r in out)


def test_too_many_negatives_fail_the_batch():
    # ...but a batch drowning in negatives trips the unusable threshold.
    records = [_record(), _record(value=-5.0), _record(value=-999.0)]
    with pytest.raises(ValueError):
        validate(records)


def test_unknown_parameter_rejected():
    with pytest.raises(pa.errors.SchemaError):
        validate([_record(parameter="benzene")])


def test_empty_batch_raises():
    with pytest.raises(ValueError):
        validate([])


def test_batch_over_null_threshold_rejected():
    # 2 of 3 rows unusable (null value) -> 67% > 20% limit
    records = [_record(), _record(value=None), _record(value=None)]
    with pytest.raises(ValueError):
        validate(records)


def test_unconvertible_units_count_toward_threshold():
    # ppm on a particulate can't be converted -> dropped -> 50% > 20% limit
    records = [_record(), _record(unit="ppm")]
    with pytest.raises(ValueError):
        validate(records)
