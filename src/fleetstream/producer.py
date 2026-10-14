"""Stream simulated GPS pings into Kafka.

    python -m fleetstream.producer --vehicles 50 --speedup 30

--speedup 30 means 30 simulated seconds per real second (50 vehicles -> ~150 events/s).
Events are keyed by vehicle_id so each vehicle's pings stay ordered within a partition.
"""
from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import asdict
from pathlib import Path

from .config import KafkaSettings
from .simulator import PING_S, FleetSimulator, add_transport_noise
from .transport import KafkaProducer, ensure_topics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=50)
    ap.add_argument("--minutes", type=float, default=600, help="simulated minutes to stream")
    ap.add_argument("--speedup", type=float, default=30)
    ap.add_argument("--noise", action="store_true", help="add duplicates and late events")
    ap.add_argument("--truth", default="data/ground_truth.jsonl")
    a = ap.parse_args()

    cfg = KafkaSettings()
    ensure_topics(cfg)
    prod = KafkaProducer(cfg)
    sim = FleetSimulator(a.vehicles)
    events = sim.run(a.minutes)
    if a.noise:
        events = add_transport_noise(events, random.Random(1))

    tick_real_s = PING_S / a.speedup
    last_sim_ts, sent, t0 = None, 0, time.time()
    for ev in events:
        if last_sim_ts and ev.ts > last_sim_ts:      # pace by simulated clock
            time.sleep(tick_real_s)
        last_sim_ts = max(last_sim_ts or ev.ts, ev.ts)
        prod.send(cfg.raw_topic, ev.vehicle_id, ev.to_json())
        sent += 1
        if sent % 5000 == 0:
            print(f"{sent:,} events sent ({sent / (time.time() - t0):.0f}/s), sim time {ev.ts}")
    prod.flush()
    Path(a.truth).parent.mkdir(parents=True, exist_ok=True)
    with open(a.truth, "w") as f:
        for ep in sim.episodes:
            f.write(json.dumps({**asdict(ep), "start": ep.start.isoformat(), "end": ep.end.isoformat()}) + "\n")
    print(f"Done: {sent:,} events, {len(sim.episodes)} labelled episodes -> {a.truth}")


if __name__ == "__main__":
    main()
