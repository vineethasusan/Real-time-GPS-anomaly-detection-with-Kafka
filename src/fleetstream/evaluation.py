"""Score detected alerts against the simulator's ground-truth episodes."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from .models import Alert
from .simulator import Episode

# How long after an episode starts an alert may still be credited to it
TOLERANCE = {"signal_gap": timedelta(minutes=20), "long_idle": timedelta(minutes=30)}
DEFAULT_TOL = timedelta(minutes=5)


def evaluate(alerts: list[Alert], episodes: list[Episode]) -> dict:
    by_key = defaultdict(list)
    for ep in episodes:
        by_key[(ep.vehicle_id, ep.type)].append(ep)

    matched_eps, tp_alerts = set(), 0
    per_type = defaultdict(lambda: {"episodes": 0, "detected": 0, "alerts": 0, "true_alerts": 0, "delays_s": []})
    for ep in episodes:
        per_type[ep.type]["episodes"] += 1

    for a in alerts:
        pt = per_type[a.type]
        pt["alerts"] += 1
        started = datetime.fromisoformat(a.started_at)
        detected = datetime.fromisoformat(a.detected_at)
        tol = TOLERANCE.get(a.type, DEFAULT_TOL)
        hit = None
        for ep in by_key.get((a.vehicle_id, a.type), []):
            if ep.start - timedelta(seconds=30) <= started <= ep.end + tol:
                hit = ep
                break
        if hit is not None:
            tp_alerts += 1
            pt["true_alerts"] += 1
            if id(hit) not in matched_eps:
                matched_eps.add(id(hit))
                pt["detected"] += 1
                pt["delays_s"].append(max(0.0, (detected - hit.start).total_seconds()))

    rows = {}
    for t, v in sorted(per_type.items()):
        d = sorted(v["delays_s"])
        rows[t] = {
            "episodes": v["episodes"], "detected": v["detected"], "alerts": v["alerts"],
            "recall": round(v["detected"] / v["episodes"], 3) if v["episodes"] else None,
            "precision": round(v["true_alerts"] / v["alerts"], 3) if v["alerts"] else None,
            "median_detection_delay_s": d[len(d) // 2] if d else None,
        }
    total_eps = len(episodes)
    return {
        "overall_recall": round(len(matched_eps) / total_eps, 3) if total_eps else None,
        "overall_precision": round(tp_alerts / len(alerts), 3) if alerts else None,
        "by_type": rows,
    }
