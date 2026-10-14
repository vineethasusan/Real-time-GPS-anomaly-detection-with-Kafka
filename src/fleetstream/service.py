"""Detector service: Kafka in -> anomaly detection -> alerts to Kafka + dashboard store.

    python -m fleetstream.service

Delivery semantics: offsets are committed only after alerts and positions are durably
stored. A crash replays from the last commit; deterministic alert ids make the replay
idempotent (INSERT OR IGNORE), so no alert is lost or duplicated.
"""
from __future__ import annotations

import signal
import time

from .config import STORE_PATH, KafkaSettings
from .detector import AnomalyDetector
from .store import Store
from .transport import KafkaEventSource, KafkaProducer, ensure_topics


def main():
    cfg = KafkaSettings()
    ensure_topics(cfg)
    source, sink = KafkaEventSource(cfg), KafkaProducer(cfg)
    store, det = Store(STORE_PATH), AnomalyDetector()
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    t0, last_report = time.time(), time.time()
    print(f"Detector consuming '{cfg.raw_topic}' from {cfg.bootstrap}")
    for batch in source.batches():
        alerts = []
        for ev in batch:
            alerts += det.process(ev)
            pos = det.trusted_position(ev.vehicle_id)
            if pos is not None:
                store.update_position(pos)
        for a in alerts:
            sink.send(cfg.alerts_topic, a.vehicle_id, a.to_json())
        store.add_alerts(alerts)
        elapsed = max(time.time() - t0, 1e-6)
        store.set_stats({**det.stats, "events_per_s": det.stats["received"] / elapsed,
                         "vehicles": len(det.state), "updated_at": time.time()})
        store.commit()
        sink.flush()
        if batch:
            source.commit()
        if time.time() - last_report > 10:
            print(det.stats)
            last_report = time.time()
        if not running:
            break
    source.close()


if __name__ == "__main__":
    main()
