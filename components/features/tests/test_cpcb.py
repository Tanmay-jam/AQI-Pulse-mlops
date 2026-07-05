"""CPCB AQI math tests — breakpoints, interpolation, reporting rule."""
import pytest

from features.src import cpcb


def test_sub_index_band_edges():
    assert cpcb.sub_index("pm25", 0) == 0
    assert cpcb.sub_index("pm25", 30) == 50
    assert cpcb.sub_index("pm25", 60) == 100
    assert cpcb.sub_index("pm10", 100) == 100
    assert cpcb.sub_index("co", 1) == 50  # mg/m³


def test_sub_index_interpolates_within_band():
    # PM2.5 45 µg/m³ is halfway through 30-60 -> halfway through 50-100
    assert cpcb.sub_index("pm25", 45) == 75


def test_sub_index_clamps_at_500():
    assert cpcb.sub_index("pm25", 1000) == 500
    assert cpcb.sub_index("so2", 99999) == 500


def test_sub_index_none_and_nan():
    assert cpcb.sub_index("pm25", None) is None
    assert cpcb.sub_index("pm25", float("nan")) is None
    assert cpcb.sub_index("pm25", -1) is None


def test_aqi_is_max_sub_index_with_dominant():
    result = cpcb.aqi({"pm25": 180.0, "pm10": 120.0, "no2": 60.0})
    assert result == (180.0, "pm25")


def test_aqi_requires_three_pollutants():
    assert cpcb.aqi({"pm25": 100.0, "no2": 50.0}) is None


def test_aqi_requires_a_pm_sub_index():
    assert cpcb.aqi({"no2": 60.0, "so2": 40.0, "co": 30.0}) is None


def test_delhi_smog_scenario():
    # Severe episode: PM2.5 24-h avg 300 µg/m³ -> sub-index in the 401-500 band
    si = cpcb.sub_index("pm25", 300)
    assert 400 < si <= 500
    aqi_value, dominant = cpcb.aqi({"pm25": si, "pm10": 350.0, "no2": 90.0})
    assert aqi_value == si
    assert dominant == "pm25"


@pytest.mark.parametrize("parameter", list(cpcb._CONC))
def test_monotonic_over_full_range(parameter):
    values = [cpcb.sub_index(parameter, c) for c in range(0, 2000, 5)]
    assert all(a <= b for a, b in zip(values, values[1:]) if a is not None)
