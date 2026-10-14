"""Unit tests for each detection rule, using hand-built event sequences."""
from datetime import datetime, timedelta

from fleetstream.config import Thresholds
from fleetstream.detector import AnomalyDetector

T0 = datetime(2026, 10, 14, 8, 0, 0)
ZONE = {"Test zone": [(25.10, 55.10), (25.11, 55.10), (25.11, 55.11), (25.10, 55.11)]}


def ev(i, speed=60.0, lat=25.0, lon=55.0, ign=True, limit=80.0, vid="V1", sec=10, eid=None):
    from fleetstream.models import GpsEvent
    return GpsEvent(eid or f"{vid}-{i}", vid, (T0 + timedelta(seconds=i * sec)).isoformat(), lat, lon,
                    speed, 0.0, ign, limit)


def run(events, geofences=None, th=None):
    d = AnomalyDetector(th or Thresholds(), geofences if geofences is not None else {})
    out = []
    for e in events:
        out += d.process(e)
    return out + d.flush(), d


def moving(n, start=0, speed=60.0, lat0=25.0):
    # ~60 km/h north: 10 s = 0.1667 km = 0.0015 deg lat
    return [ev(start + i, speed=speed, lat=lat0 + 0.0015 * i) for i in range(n)]


def types(alerts):
    return [a.type for a in alerts]


def test_normal_driving_raises_nothing():
    alerts, _ = run(moving(60))
    assert alerts == []


def test_overspeed_needs_sustained_excess_and_alerts_once():
    seq = moving(5) + [ev(5 + i, speed=120, lat=25.0075 + 0.003 * i) for i in range(10)]
    seq += [ev(15 + i, speed=s, lat=25.04 + 0.002 * i) for i, s in enumerate([100, 80, 60, 60, 60])]  # gentle slow-down
    alerts, _ = run(seq)
    assert types(alerts) == ["overspeed"]
    assert alerts[0].details["excess_kmh"] == 40.0


def test_two_ping_spike_is_not_overspeed():
    seq = moving(5) + [ev(5, speed=120, lat=25.0075), ev(6, speed=120, lat=25.0105)] + moving(5, 7, lat0=25.012)
    assert "overspeed" not in types(run(seq)[0])


def slow_to_stop(start, lat):
    return [ev(start + i, speed=s, lat=lat) for i, s in enumerate([40, 20])]


def test_long_idle_engine_on():
    seq = moving(3) + slow_to_stop(3, 25.003) + [ev(5 + i, speed=0, lat=25.003) for i in range(70)]  # ~11.5 min
    alerts, _ = run(seq)
    assert types(alerts) == ["long_idle"]


def test_parked_with_engine_off_is_not_idle():
    seq = moving(3) + slow_to_stop(3, 25.003) + [ev(5 + i, speed=0, lat=25.003, ign=False) for i in range(90)]
    assert run(seq)[0] == []


def test_harsh_braking():
    seq = moving(5, speed=90) + [ev(5, speed=20, lat=25.0076)] + moving(3, 6, speed=20, lat0=25.0077)
    alerts, _ = run(seq)
    assert "harsh_braking" in types(alerts)


def test_gps_jump_is_alerted_and_ignored():
    seq = moving(5) + [ev(5, lat=25.3, lon=55.3)] + moving(5, 6, lat0=25.009)
    alerts, d = run(seq)
    assert types(alerts) == ["gps_jump"]
    assert abs(d.trusted_position("V1").lat - 25.009 - 0.0015 * 4) < 1e-6   # bad fix never became "trusted"


def test_real_relocation_is_accepted_after_two_consistent_fixes():
    seq = moving(5) + [ev(5 + i, lat=25.3 + 0.0015 * i, lon=55.3) for i in range(5)]
    alerts, d = run(seq)
    assert types(alerts) == ["gps_jump"]                  # one alert, not one per ping
    assert d.trusted_position("V1").lat > 25.29


def test_geofence_entry_alerts_once():
    path = [ev(i, lat=25.095 + 0.002 * i, lon=55.105) for i in range(10)]   # drives into and through the zone
    alerts, _ = run(path, geofences=ZONE)
    assert types(alerts) == ["geofence_breach"]
    assert alerts[0].details["zone"] == "Test zone"


def test_signal_gap_uses_fleet_watermark():
    v1 = moving(10)                                        # V1 goes silent after 100 s
    v2 = [ev(i, vid="V2", lat=25.2 + 0.0015 * i) for i in range(60)]   # V2 keeps reporting for 10 min
    alerts, _ = run(sorted(v1 + v2, key=lambda e: e.ts))
    gaps = [a for a in alerts if a.type == "signal_gap"]
    assert len(gaps) == 1 and gaps[0].vehicle_id == "V1"
