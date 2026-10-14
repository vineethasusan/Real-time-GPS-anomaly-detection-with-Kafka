"""Run the whole pipeline in one process, no Kafka needed (demo, tests, CI).

    python -m fleetstream.run_local --vehicles 50 --minutes 480            # as fast as possible + evaluation
    python -m fleetstream.run_local --vehicles 50 --minutes 600 --speedup 30  # paced, for the live dashboard
"""
from __future__ import annotations

import argparse
import json
import random
import time

from .config import STORE_PATH
from .detector import AnomalyDetector
from .evaluation import evaluate
from .simulator import PING_S, FleetSimulator, add_transport_noise
from .store import Store


def run(vehicles: int = 50, minutes: float = 480, speedup: float | None = None, seed: int = 7,
        store_path: str | None = STORE_PATH, noise: bool = True):
    sim = FleetSimulator(vehicles, seed=seed)
    det = AnomalyDetector()
    store = Store(store_path) if store_path else None
    if store:
        store.reset()
    events = sim.run(minutes)
    if noise:
        events = add_transport_noise(events, random.Random(seed))
    alerts, last_ts, t0, n = [], None, time.time(), 0
    for ev in events:
        if speedup and last_ts and ev.ts > last_ts:
            time.sleep(PING_S / speedup)
        last_ts = max(last_ts or ev.ts, ev.ts)
        new = det.process(ev)
        alerts += new
        n += 1
        if store:
            pos = det.trusted_position(ev.vehicle_id)
            if pos is not None:
                store.update_position(pos)
            store.add_alerts(new)
            if n % 500 == 0:
                store.set_stats({**det.stats, "events_per_s": n / max(time.time() - t0, 1e-6),
                                 "vehicles": len(det.state), "updated_at": time.time()})
                store.commit()
    final = det.flush()
    alerts += final
    elapsed = time.time() - t0
    if store:
        store.add_alerts(final)
        store.set_stats({**det.stats, "events_per_s": n / max(elapsed, 1e-6), "vehicles": len(det.state),
                         "updated_at": time.time()})
        store.commit()
    return det, alerts, sim.episodes, elapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=50)
    ap.add_argument("--minutes", type=float, default=480)
    ap.add_argument("--speedup", type=float, help="pace the stream (simulated seconds per real second)")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    det, alerts, episodes, elapsed = run(a.vehicles, a.minutes, a.speedup, a.seed)
    rep = evaluate(alerts, episodes)
    s = det.stats
    print(f"Processed {s['processed']:,} events in {elapsed:.1f}s ({s['received'] / elapsed:,.0f} events/s), "
          f"{s['duplicates']} duplicates removed, {s['late_dropped']} late events dropped")
    print(f"Alerts: {len(alerts)} | recall {rep['overall_recall']:.1%} | precision {rep['overall_precision']:.1%}")
    for t, r in rep["by_type"].items():
        print(f"  {t:<16} episodes {r['episodes']:>3}  recall {r['recall']:.0%}  precision "
              f"{(r['precision'] or 0):.0%}  median delay {r['median_detection_delay_s']}s")
    with open("data/evaluation.json", "w") as f:
        json.dump({"stats": s, "elapsed_s": round(elapsed, 2), **rep}, f, indent=2)


if __name__ == "__main__":
    main()
