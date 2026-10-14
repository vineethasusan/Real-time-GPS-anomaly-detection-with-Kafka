"""Event and alert schemas shared by producer, detector and dashboard."""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass
class GpsEvent:
    event_id: str
    vehicle_id: str
    ts: str                  # ISO-8601 event time (device clock)
    lat: float
    lon: float
    speed_kmh: float
    heading: float
    ignition: bool
    road_limit_kmh: float    # speed limit of the road segment (from map matching)
    injected: str | None = None  # ground-truth label for evaluation only; real devices don't send this

    @property
    def time(self) -> datetime:
        return datetime.fromisoformat(self.ts)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> GpsEvent:
        return cls(**json.loads(raw))


@dataclass
class Alert:
    alert_id: str
    vehicle_id: str
    type: str                # overspeed | long_idle | signal_gap | geofence_breach | gps_jump | harsh_braking
    severity: str            # low | medium | high
    started_at: str
    detected_at: str
    lat: float
    lon: float
    details: dict = field(default_factory=dict)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def point_in_polygon(lat: float, lon: float, polygon: list[tuple[float, float]]) -> bool:
    """Ray casting; polygon is a list of (lat, lon)."""
    inside = False
    j = len(polygon) - 1
    for i in range(len(polygon)):
        yi, xi = polygon[i]
        yj, xj = polygon[j]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi + 1e-12) + xi:
            inside = not inside
        j = i
    return inside
