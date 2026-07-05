"""Tests for the live-path record filters (pure functions, no network)."""
import datetime as dt

from ingest.src.openaq_client import dedupe_twin_sensors, drop_stale


def _iso(hours_ago: float) -> str:
    ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours_ago)
    return ts.isoformat().replace("+00:00", "Z")


def _rec(station=17, param="pm25", hours_ago=0.5, value=45.0):
    return {
        "station_id": station,
        "parameter": param,
        "value": value,
        "timestamp_utc": _iso(hours_ago),
    }


def test_drop_stale_keeps_fresh_drops_zombies():
    records = [
        _rec(hours_ago=0.5),            # fresh
        _rec(hours_ago=2.9),            # inside 3h window
        _rec(hours_ago=5),              # stale
        _rec(hours_ago=24 * 365 * 8),   # 2018-style zombie
    ]
    kept = drop_stale(records, max_age_hours=3)
    assert len(kept) == 2


def test_drop_stale_drops_unparseable_timestamps():
    assert drop_stale([{"timestamp_utc": None}, {"timestamp_utc": "garbage"}]) == []


def test_dedupe_keeps_newest_twin():
    old_twin = _rec(hours_ago=2.0, value=100.0)
    new_twin = _rec(hours_ago=0.5, value=45.0)
    kept = dedupe_twin_sensors([old_twin, new_twin])
    assert len(kept) == 1
    assert kept[0]["value"] == 45.0


def test_dedupe_is_per_station_and_parameter():
    records = [
        _rec(station=17, param="pm25"),
        _rec(station=17, param="pm10"),
        _rec(station=50, param="pm25"),
    ]
    assert len(dedupe_twin_sensors(records)) == 3
