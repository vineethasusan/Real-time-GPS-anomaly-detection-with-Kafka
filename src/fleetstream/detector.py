"""Stateful, event-time anomaly detector.

Streaming concerns handled explicitly:
  * duplicates   - at-least-once delivery is deduplicated by event_id
  * out-of-order - events are buffered per vehicle and released in event-time
                   order once they are older than the allowed lateness
  * watermark    - the fleet-wide event-time watermark drives time-based rules
                   (signal gap) without relying on the wall clock, so replays and
                   backfills give the same answer as live processing
  * bad fixes    - a physically impossible jump is alerted and then ignored, so it
                   does not corrupt speed/position state for the next ping

Rules (one alert per episode, never one per ping):
  overspeed, harsh_braking, long_idle, geofence_breach, gps_jump, signal_gap
"""
from __future__ import annotations

import hashlib
import heapq
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .config import GEOFENCES, Thresholds
from .models import Alert, GpsEvent, haversine_km, point_in_polygon


@dataclass
class VehicleState:
    last: GpsEvent | None = None            # last *trusted* event
    last_time: datetime | None = None
    overspeed_run: int = 0
    overspeed_active: bool = False
    idle_since: datetime | None = None
    idle_alerted: bool = False
    zones: set = field(default_factory=set)
    gap_alerted: bool = False
    suspect: GpsEvent | None = None         # last fix rejected as a jump
    buffer: list = field(default_factory=list)  # heap of (time, seq, event)


class AnomalyDetector:
    def __init__(self, thresholds: Thresholds | None = None, geofences: dict | None = None):
        self.th = thresholds or Thresholds()
        self.geofences = geofences if geofences is not None else GEOFENCES
        self.state: dict[str, VehicleState] = {}
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.watermark: datetime | None = None
        self.max_event_time: datetime | None = None
        self._seq = 0
        self.stats = {"received": 0, "duplicates": 0, "late_dropped": 0, "processed": 0, "alerts": 0}

    # ---------------------------------------------------------------- input
    def process(self, ev: GpsEvent) -> list[Alert]:
        """Feed one raw event; returns the alerts that became final as a result."""
        self.stats["received"] += 1
        if ev.event_id in self.seen:
            self.stats["duplicates"] += 1
            return []
        self.seen[ev.event_id] = None
        if len(self.seen) > self.th.dedupe_window:
            self.seen.popitem(last=False)

        t = ev.time
        if self.watermark and t < self.watermark:
            self.stats["late_dropped"] += 1          # later than allowed lateness: count it, don't reprocess
            return []
        if self.max_event_time is None or t > self.max_event_time:
            self.max_event_time = t
            self.watermark = t - timedelta(seconds=self.th.allowed_lateness_s)

        st = self.state.setdefault(ev.vehicle_id, VehicleState())
        self._seq += 1
        heapq.heappush(st.buffer, (t, self._seq, ev))
        return self._release()

    def trusted_position(self, vehicle_id: str) -> GpsEvent | None:
        st = self.state.get(vehicle_id)
        return st.last if st else None

    def flush(self) -> list[Alert]:
        """End of stream: process everything still buffered."""
        self.watermark = datetime.max
        out = self._release()
        return out

    # ------------------------------------------------------------- internals
    def _release(self) -> list[Alert]:
        alerts: list[Alert] = []
        for vid, st in self.state.items():
            while st.buffer and st.buffer[0][0] <= self.watermark:
                _, _, ev = heapq.heappop(st.buffer)
                alerts += self._apply(vid, st, ev)
            alerts += self._check_gap(vid, st)
        self.stats["alerts"] += len(alerts)
        return alerts

    def _apply(self, vid: str, st: VehicleState, ev: GpsEvent) -> list[Alert]:
        self.stats["processed"] += 1
        out: list[Alert] = []
        t = ev.time
        prev = st.last

        if prev is not None:
            dt_s = (t - st.last_time).total_seconds()
            if dt_s <= 0:
                return out
            # --- GPS jump: impossible implied speed => corrupted fix, alert and ignore it
            implied = haversine_km(prev.lat, prev.lon, ev.lat, ev.lon) / (dt_s / 3600)
            if implied > self.th.gps_jump_kmh and dt_s < 600:
                sus = st.suspect
                if sus is not None and self._plausible(sus, ev):
                    # Two consecutive fixes agree with each other, not with the old position:
                    # the vehicle really is here (e.g. tracker reboot, towed). Re-anchor silently.
                    st.suspect = None
                else:
                    st.suspect = ev
                    out.append(self._alert(vid, "gps_jump", "low", t, t, ev, implied_speed_kmh=round(implied),
                                           jump_km=round(implied * dt_s / 3600, 1)))
                    return out
            else:
                st.suspect = None
            # --- Harsh braking
            if dt_s <= 15 and prev.speed_kmh - ev.speed_kmh > 0:
                decel = (prev.speed_kmh - ev.speed_kmh) / dt_s
                if decel >= self.th.harsh_brake_kmh_per_s:
                    sev = "high" if decel >= 7 else "medium"
                    out.append(self._alert(vid, "harsh_braking", sev, st.last_time, t, ev,
                                           from_kmh=prev.speed_kmh, to_kmh=ev.speed_kmh,
                                           decel_kmh_per_s=round(decel, 1)))

        # --- Overspeed: sustained, one alert per episode
        excess = ev.speed_kmh - ev.road_limit_kmh
        if excess > self.th.overspeed_margin_kmh:
            st.overspeed_run += 1
            if st.overspeed_run >= self.th.overspeed_min_pings and not st.overspeed_active:
                st.overspeed_active = True
                sev = "high" if excess > 40 else "medium" if excess > 25 else "low"
                started = t - timedelta(seconds=10 * (st.overspeed_run - 1))
                out.append(self._alert(vid, "overspeed", sev, started, t, ev, speed_kmh=ev.speed_kmh,
                                       limit_kmh=ev.road_limit_kmh, excess_kmh=round(excess, 1)))
        elif excess <= self.th.overspeed_clear_margin_kmh:
            st.overspeed_run, st.overspeed_active = 0, False

        # --- Long idle: engine on, not moving
        if ev.ignition and ev.speed_kmh < self.th.idle_speed_kmh:
            st.idle_since = st.idle_since or t
            idle_min = (t - st.idle_since).total_seconds() / 60
            if idle_min >= self.th.long_idle_min and not st.idle_alerted:
                st.idle_alerted = True
                out.append(self._alert(vid, "long_idle", "medium" if idle_min < 20 else "high", st.idle_since, t,
                                       ev, idle_minutes=round(idle_min, 1),
                                       est_fuel_wasted_l=round(idle_min / 60 * 1.5, 2)))
        else:
            st.idle_since, st.idle_alerted = None, False

        # --- Geofence entry
        inside = {z for z, poly in self.geofences.items() if point_in_polygon(ev.lat, ev.lon, poly)}
        for z in sorted(inside - st.zones):
            out.append(self._alert(vid, "geofence_breach", "high", t, t, ev, zone=z))
        st.zones = inside

        st.last, st.last_time, st.gap_alerted = ev, t, False
        return out

    def _plausible(self, a: GpsEvent, b: GpsEvent) -> bool:
        dt_s = (b.time - a.time).total_seconds()
        return dt_s > 0 and haversine_km(a.lat, a.lon, b.lat, b.lon) / (dt_s / 3600) <= self.th.gps_jump_kmh

    def _check_gap(self, vid: str, st: VehicleState) -> list[Alert]:
        """A vehicle that was driving has gone silent while the rest of the fleet keeps reporting."""
        if st.last is None or st.gap_alerted or not st.last.ignition or self.watermark in (None, datetime.max):
            return []
        silent_min = (self.watermark - st.last_time).total_seconds() / 60
        if silent_min >= self.th.signal_gap_min:
            st.gap_alerted = True
            return [self._alert(vid, "signal_gap", "medium", st.last_time, self.watermark, st.last,
                                silent_minutes=round(silent_min, 1), last_speed_kmh=st.last.speed_kmh)]
        return []

    def _alert(self, vid, kind, severity, started, detected, ev, **details) -> Alert:
        # Deterministic id: replaying the same events after a crash yields the same alert ids,
        # so the sink can upsert and the pipeline is effectively exactly-once end to end.
        aid = hashlib.sha1(f"{vid}|{kind}|{started.isoformat()}".encode()).hexdigest()[:20]
        return Alert(aid, vid, kind, severity, started.isoformat(), detected.isoformat(), ev.lat, ev.lon, details)
