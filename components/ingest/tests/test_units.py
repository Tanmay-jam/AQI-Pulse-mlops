"""Unit-conversion tests: everything must land in µg/m³."""
import pytest

from ingest.src import units


def test_ugm3_passthrough():
    assert units.to_ugm3("pm25", 42.0, "µg/m³") == 42.0
    assert units.to_ugm3("pm25", 42.0, "ug/m3") == 42.0


def test_ppb_gas_conversion():
    # 1 ppb O3 = 48.00 / 24.45 µg/m³
    assert units.to_ugm3("o3", 1.0, "ppb") == pytest.approx(48.00 / 24.45)


def test_ppm_gas_conversion():
    # 1 ppm CO = 1000 ppb * 28.01 / 24.45 ≈ 1145.6 µg/m³
    assert units.to_ugm3("co", 1.0, "ppm") == pytest.approx(1145.6, abs=0.1)


def test_mgm3_converts_to_ugm3():
    assert units.to_ugm3("co", 1.14, "mg/m³") == pytest.approx(1140.0)
    assert units.to_ugm3("no2", 0.5, "mg/m3") == pytest.approx(500.0)


def test_co_ppb_mislabel_treated_as_mgm3():
    # OpenAQ labels CPCB CO "ppb" but the numbers are mg/m³: 1.14 "ppb" is
    # really 1.14 mg/m³ -> 1140 µg/m³ (not the ppb conversion's ~1.3)
    assert units.to_ugm3("co", 1.14, "ppb") == pytest.approx(1140.0)


def test_genuine_co_ppb_still_converts():
    # A real CO ppb reading (>50) uses the molar conversion:
    # 500 ppb * 28.01 / 24.45 ≈ 572.8 µg/m³
    assert units.to_ugm3("co", 500.0, "ppb") == pytest.approx(572.8, abs=0.1)


def test_mislabel_threshold_is_co_specific():
    # Low ppb on other gases is legitimate and must use molar conversion
    assert units.to_ugm3("no2", 10.0, "ppb") == pytest.approx(10 * 46.01 / 24.45)


def test_unconvertible_returns_none():
    assert units.to_ugm3("pm25", 1.0, "ppm") is None  # gas unit on particulate
    assert units.to_ugm3("no2", 1.0, "mystery") is None


def test_normalize_drops_unconvertible_and_keeps_nulls():
    records = [
        {"parameter": "no2", "value": 10.0, "unit": "ppb"},
        {"parameter": "pm25", "value": 5.0, "unit": "ppm"},   # dropped
        {"parameter": "pm25", "value": None, "unit": "µg/m³"},  # kept for validator
    ]
    out = units.normalize(records)
    assert len(out) == 2
    assert all(r["unit"] == units.CANONICAL_UNIT for r in out if r["value"] is not None)
    # input not mutated
    assert records[0]["unit"] == "ppb"
