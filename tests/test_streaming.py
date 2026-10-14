"""Streaming semantics: duplicates, out-of-order events, late events, idempotent alert ids."""
import random

from test_rules import ev, moving, run

from fleetstream.config import Thresholds


def test_duplicates_are_ignored():
    seq = moving(5)
    seq = seq + [seq[2], seq[3]]
    alerts, d = run(seq)
    assert d.stats["duplicates"] == 2 and d.stats["processed"] == 5


def test_out_of_order_within_lateness_gives_same_result():
    base = moving(5) + [ev(5 + i, speed=120, lat=25.0075 + 0.003 * i) for i in range(6)] + moving(5, 11, lat0=25.03)
    shuffled = base[:]
    random.Random(0).shuffle(shuffled[3:12])
    a1, _ = run(base)
    a2, _ = run(shuffled)
    assert [a.alert_id for a in a1] == [a.alert_id for a in a2]


def test_events_later_than_allowed_lateness_are_dropped_and_counted():
    seq = moving(30)
    late = seq.pop(2)
    seq.append(late)                                   # arrives ~280 s late, lateness is 60 s
    _, d = run(seq, th=Thresholds(allowed_lateness_s=60))
    assert d.stats["late_dropped"] == 1


def test_alert_ids_are_deterministic_for_replays():
    seq = moving(5) + [ev(5 + i, speed=120, lat=25.0075 + 0.003 * i) for i in range(6)]
    assert [a.alert_id for a in run(seq)[0]] == [a.alert_id for a in run(seq)[0]]
