"""Thresholds, zones and connection settings (override any value with an environment variable)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default):
    v = os.getenv(name)
    return type(default)(v) if v is not None else default


# Restricted zones as (lat, lon) polygons. Illustrative areas in Dubai.
GEOFENCES: dict[str, list[tuple[float, float]]] = {
    "Port restricted area (Jebel Ali)": [(25.000, 55.020), (25.035, 55.020), (25.035, 55.075), (25.000, 55.075)],
    "No-heavy-vehicle zone (Downtown)": [(25.190, 55.270), (25.200, 55.270), (25.200, 55.282), (25.190, 55.282)],
    "Airport perimeter (DXB)": [(25.242, 55.355), (25.254, 55.355), (25.254, 55.372), (25.242, 55.372)],
}


@dataclass(frozen=True)
class Thresholds:
    overspeed_margin_kmh: float = field(default_factory=lambda: _env("OVERSPEED_MARGIN_KMH", 15.0))
    overspeed_min_pings: int = field(default_factory=lambda: _env("OVERSPEED_MIN_PINGS", 3))
    overspeed_clear_margin_kmh: float = 5.0
    idle_speed_kmh: float = 3.0
    long_idle_min: float = field(default_factory=lambda: _env("LONG_IDLE_MIN", 10.0))
    signal_gap_min: float = field(default_factory=lambda: _env("SIGNAL_GAP_MIN", 5.0))
    gps_jump_kmh: float = 250.0               # implied speed between two pings that is physically impossible
    harsh_brake_kmh_per_s: float = 4.5        # ~0.13 g
    allowed_lateness_s: float = field(default_factory=lambda: _env("ALLOWED_LATENESS_S", 60.0))
    dedupe_window: int = 200_000              # remembered event ids


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:19092"))
    raw_topic: str = field(default_factory=lambda: _env("RAW_TOPIC", "gps.raw"))
    alerts_topic: str = field(default_factory=lambda: _env("ALERTS_TOPIC", "fleet.alerts"))
    group_id: str = field(default_factory=lambda: _env("GROUP_ID", "anomaly-detector"))


STORE_PATH = _env("STORE_PATH", "data/stream.db")
