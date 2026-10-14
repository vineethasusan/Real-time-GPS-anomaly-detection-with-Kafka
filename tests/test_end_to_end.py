"""Simulator -> noisy transport -> detector -> store, scored against ground truth."""
from fleetstream.evaluation import evaluate
from fleetstream.run_local import run


def test_pipeline_detects_injected_anomalies(tmp_path):
    det, alerts, episodes, _ = run(vehicles=25, minutes=240, store_path=str(tmp_path / "s.db"), seed=3)
    rep = evaluate(alerts, episodes)
    assert det.stats["duplicates"] > 0                 # noise was really injected
    assert rep["overall_recall"] >= 0.9
    assert rep["overall_precision"] >= 0.9


def test_store_is_populated(tmp_path):
    from fleetstream.store import Store

    path = tmp_path / "s.db"
    run(vehicles=10, minutes=60, store_path=str(path), seed=4)
    s = Store(path)
    assert len(s.positions()) == 10
    assert s.stats()["processed"] > 0
