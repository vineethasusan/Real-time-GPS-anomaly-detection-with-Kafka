"""GPS fleet simulator with labelled anomaly injection.

Vehicles drive between real Dubai locations, stop at destinations, and send a
GPS ping every 10 seconds of simulated time. Anomalies are injected on purpose
and recorded as ground truth, so the detector's precision and recall can be
measured, which is impossible with unlabelled real data.

Injected episodes: overspeed, long_idle, signal_gap, gps_jump, harsh_braking.
Geofence breaches are labelled from geometry (any real entry into a zone).
The transport layer can also add realistic noise: duplicates and late events.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .config import GEOFENCES
from .models import GpsEvent, haversine_km, point_in_polygon

HUBS = {
    "Jebel Ali Free Zone": (25.010, 55.100), "Dubai Investments Park": (24.985, 55.170),
    "Dubai Marina": (25.080, 55.140), "Al Barsha": (25.110, 55.200), "Al Quoz": (25.140, 55.230),
    "Jumeirah": (25.210, 55.245), "Business Bay": (25.178, 55.300), "Ras Al Khor": (25.190, 55.350),
    "Deira": (25.270, 55.320), "Al Qusais": (25.280, 55.380), "Mirdif": (25.220, 55.420),
    "Dubai Silicon Oasis": (25.120, 55.380), "Port restricted area": (25.018, 55.048),
}
ROAD_LIMITS = [60, 80, 80, 100, 100, 120]
PING_S = 10
ANOMALY_TYPES = ["overspeed", "long_idle", "signal_gap", "gps_jump", "harsh_braking"]


@dataclass
class Episode:
    vehicle_id: str
    type: str
    start: datetime
    end: datetime


@dataclass
class _Vehicle:
    vid: str
    lat: float
    lon: float
    dest: str
    limit: float
    speed: float = 0.0
    ignition: bool = True
    mode: str = "drive"                  # drive | stop | idle | gap | overspeed
    mode_left_s: float = 0.0
    inside: set = field(default_factory=set)
    pending_brake: bool = False


class FleetSimulator:
    def __init__(self, n_vehicles: int = 50, start: datetime | None = None, seed: int = 7,
                 anomaly_rate_per_hour: float = 0.6):
        self.rng = random.Random(seed)
        self.t = start or datetime(2026, 10, 14, 6, 0, 0)
        self.rate = anomaly_rate_per_hour
        self.episodes: list[Episode] = []
        names = [h for h in HUBS if h != "Port restricted area"]
        self.vehicles = []
        for i in range(n_vehicles):
            lat, lon = HUBS[self.rng.choice(names)]
            v = _Vehicle(f"DXB-{1000 + i}", lat + self.rng.uniform(-0.01, 0.01), lon + self.rng.uniform(-0.01, 0.01),
                         self.rng.choice(names), self.rng.choice(ROAD_LIMITS))
            v.inside = {z for z, poly in GEOFENCES.items() if point_in_polygon(v.lat, v.lon, poly)}
            self.vehicles.append(v)

    # ------------------------------------------------------------------ main
    def run(self, minutes: float):
        """Yield GpsEvents in event-time order for `minutes` of simulated time."""
        steps = int(minutes * 60 / PING_S)
        for _ in range(steps):
            self.t += timedelta(seconds=PING_S)
            for v in self.vehicles:
                ev = self._step(v)
                if ev is not None:
                    yield ev

    # ------------------------------------------------------------- per tick
    def _step(self, v: _Vehicle) -> GpsEvent | None:
        p_anomaly = self.rate / 3600 * PING_S
        if v.mode == "drive" and self.rng.random() < p_anomaly:
            self._start_anomaly(v)

        label = None
        if v.mode == "gap":
            self._move(v)
            v.mode_left_s -= PING_S
            if v.mode_left_s <= 0:
                v.mode = "drive"
            return None                                # device silent (tunnel, unplugged tracker…)

        if v.mode == "idle":                           # pulls over smoothly, then engine on, not moving
            v.speed, v.ignition, label = max(0.0, v.speed - 25), True, "long_idle"
        elif v.mode == "stop":                         # normal delivery stop: brake gently, then engine off
            v.speed = max(0.0, v.speed - 25)
            v.ignition = v.speed > 0
        elif v.mode == "overspeed":
            v.speed = v.limit + self.rng.uniform(25, 45)
            label = "overspeed"
            self._move(v)
        else:                                          # normal driving, never above limit + 8
            if v.pending_brake:
                v.speed, v.pending_brake, label = max(0.0, v.speed - self.rng.uniform(50, 65)), False, "harsh_braking"
            else:
                target = min(v.limit + 8, max(15.0, self.rng.gauss(v.limit * 0.85, 8)))
                if haversine_km(v.lat, v.lon, *HUBS[v.dest]) < 1.5:
                    target = min(target, 25.0)                       # slow down approaching the stop
                v.speed += max(-25.0, min(25.0, target - v.speed))   # smooth acceleration/braking
            v.ignition = True
            arrived = self._move(v)
            if arrived:
                v.mode, v.mode_left_s = "stop", self.rng.uniform(5, 25) * 60

        if v.mode in ("idle", "stop", "overspeed"):
            v.mode_left_s -= PING_S
            if v.mode_left_s <= 0:
                if v.mode == "stop":
                    # occasionally a driver takes an unauthorised route into the port zone
                    v.dest = ("Port restricted area" if self.rng.random() < 0.04 else
                              self.rng.choice([h for h in HUBS if h != "Port restricted area"]))
                    v.limit = self.rng.choice(ROAD_LIMITS)
                v.mode = "drive"
                v.ignition = True

        lat, lon = v.lat, v.lon
        if self._jump_now(v):
            # a single corrupted fix ~20-40 km away; the vehicle itself has not moved
            ang = self.rng.uniform(0, 2 * math.pi)
            d = self.rng.uniform(0.2, 0.35)
            lat, lon, label = lat + d * math.cos(ang), lon + d * math.sin(ang), "gps_jump"

        self._check_geofences(v)
        return GpsEvent(uuid.uuid4().hex, v.vid, self.t.isoformat(), round(lat, 6), round(lon, 6),
                        round(v.speed, 1), 0.0, v.ignition, float(v.limit), label)

    # -------------------------------------------------------------- helpers
    def _start_anomaly(self, v: _Vehicle) -> None:
        kind = self.rng.choice(ANOMALY_TYPES)
        if kind == "harsh_braking" and v.speed < 55:
            kind = "overspeed"                       # need real speed to brake hard from
        if kind == "overspeed":
            dur = self.rng.uniform(1, 3) * 60
            v.mode, v.mode_left_s = "overspeed", dur
        elif kind == "long_idle":
            dur = self.rng.uniform(12, 25) * 60
            v.mode, v.mode_left_s = "idle", dur
        elif kind == "signal_gap":
            dur = self.rng.uniform(7, 15) * 60
            v.mode, v.mode_left_s = "gap", dur
        elif kind == "gps_jump":
            v.mode_left_s, dur = -1, 0               # handled on this tick by _jump_now
            self._jump_pending = getattr(self, "_jump_pending", set()) | {v.vid}
        else:  # harsh_braking
            v.pending_brake, dur = True, PING_S
        start = self.t
        self.episodes.append(Episode(v.vid, kind, start, start + timedelta(seconds=max(dur, PING_S))))

    def _jump_now(self, v: _Vehicle) -> bool:
        pending = getattr(self, "_jump_pending", set())
        if v.vid in pending:
            pending.discard(v.vid)
            return True
        return False

    def _move(self, v: _Vehicle) -> bool:
        dlat, dlon = HUBS[v.dest]
        dist = haversine_km(v.lat, v.lon, dlat, dlon)
        step = v.speed * PING_S / 3600
        if dist <= step or dist < 0.05:
            v.lat, v.lon = dlat, dlon
            return True
        f = step / dist
        v.lat += (dlat - v.lat) * f
        v.lon += (dlon - v.lon) * f
        return False

    def _check_geofences(self, v: _Vehicle) -> None:
        now = {z for z, poly in GEOFENCES.items() if point_in_polygon(v.lat, v.lon, poly)}
        for z in now - v.inside:
            self.episodes.append(Episode(v.vid, "geofence_breach", self.t, self.t + timedelta(seconds=PING_S)))
        v.inside = now


def add_transport_noise(events, rng: random.Random, dup_rate: float = 0.01, late_rate: float = 0.01,
                        max_delay_events: int = 150):
    """Simulate an unreliable network: duplicate deliveries and events arriving late / out of order."""
    buffer = []
    for i, ev in enumerate(events):
        r = rng.random()
        if r < late_rate:
            buffer.append((i + rng.randint(10, max_delay_events), ev))
        else:
            yield ev
            if r < late_rate + dup_rate:
                yield ev                               # at-least-once delivery
        due = [b for b in buffer if b[0] <= i]
        for b in due:
            buffer.remove(b)
            yield b[1]
    for _, ev in sorted(buffer, key=lambda b: b[0]):
        yield ev
