from datetime import UTC, datetime

from processing import apply_events, default_stats, settled


def test_apply_events():
    s = default_stats()
    apply_events(
        s,
        [{"rainfall_mm": 5.0, "recorded_timestamp": "t1"}, {"rainfall_mm": 9.0, "recorded_timestamp": "t2"}],
        [{"moisture_percent": 10.0, "recorded_timestamp": "t3"}],
    )
    apply_events(s, [], [{"moisture_percent": 30.0, "recorded_timestamp": "t4"}])
    assert s["num_precipitation_readings"] == 2 and s["max_rainfall_mm"] == 9.0
    assert s["num_soil_moisture_readings"] == 2 and s["avg_moisture_percent"] == 20.0
    assert s["last_updated_precipitation"] == "t2" and s["last_updated_soil_moisture"] == "t4"


def test_settled_holds_back_recent_events():

    cutoff = datetime(2026, 10, 28, 12, 0, 0, tzinfo=UTC)
    events = [
        {"recorded_timestamp": "2026-10-28T11:59:59.999999+00:00"},
        {"recorded_timestamp": "2026-10-28T12:00:00+00:00"},
    ]
    assert settled(events, cutoff) == events[:1]
