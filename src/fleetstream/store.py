"""Serving store for the live dashboard: alerts, latest vehicle positions and throughput stats.

SQLite in WAL mode lets the detector write while the dashboard reads.
In production this would be Postgres/TimescaleDB or a Kafka-backed state store.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .models import Alert, GpsEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY, vehicle_id TEXT, type TEXT, severity TEXT,
    started_at TEXT, detected_at TEXT, lat REAL, lon REAL, details TEXT, inserted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_detected ON alerts(detected_at);
CREATE TABLE IF NOT EXISTS positions (
    vehicle_id TEXT PRIMARY KEY, ts TEXT, lat REAL, lon REAL, speed_kmh REAL, ignition INTEGER
);
CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value REAL);
"""


class Store:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.executescript(SCHEMA)
        self._pending_pos: dict[str, tuple] = {}

    def reset(self) -> None:
        self.con.executescript("DELETE FROM alerts; DELETE FROM positions; DELETE FROM stats;")
        self.con.commit()

    def add_alerts(self, alerts: list[Alert]) -> None:
        if not alerts:
            return
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.con.executemany(
            "INSERT OR IGNORE INTO alerts VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(a.alert_id, a.vehicle_id, a.type, a.severity, a.started_at, a.detected_at, a.lat, a.lon,
              json.dumps(a.details), now) for a in alerts])

    def update_position(self, ev: GpsEvent) -> None:
        """Call with the detector's last *trusted* fix, so rejected GPS jumps never reach the map."""
        self._pending_pos[ev.vehicle_id] = (ev.vehicle_id, ev.ts, ev.lat, ev.lon, ev.speed_kmh, int(ev.ignition))

    def set_stats(self, stats: dict) -> None:
        self.con.executemany("INSERT OR REPLACE INTO stats VALUES (?,?)", [(k, float(v)) for k, v in stats.items()])

    def commit(self) -> None:
        if self._pending_pos:
            self.con.executemany("INSERT OR REPLACE INTO positions VALUES (?,?,?,?,?,?)", self._pending_pos.values())
            self._pending_pos.clear()
        self.con.commit()

    # ------------------------------------------------------------ reads
    def alerts(self, limit: int = 500) -> pd.DataFrame:
        return pd.read_sql_query("SELECT * FROM alerts ORDER BY detected_at DESC LIMIT ?", self.con, params=(limit,))

    def positions(self) -> pd.DataFrame:
        return pd.read_sql_query("SELECT * FROM positions", self.con)

    def stats(self) -> dict:
        return dict(self.con.execute("SELECT key, value FROM stats").fetchall())
