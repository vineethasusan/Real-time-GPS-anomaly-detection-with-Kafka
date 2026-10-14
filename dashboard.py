"""Live fleet operations dashboard.

    streamlit run dashboard.py
Reads the store written by the detector (local runner or Kafka service) and refreshes every 2 seconds.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fleetstream.config import GEOFENCES, STORE_PATH  # noqa: E402
from fleetstream.store import Store  # noqa: E402

st.set_page_config(page_title="Fleet Live Monitor", page_icon="📡", layout="wide")

TYPE_LABEL = {"overspeed": "Overspeeding", "long_idle": "Long idle", "signal_gap": "Signal lost",
              "geofence_breach": "Restricted zone", "gps_jump": "GPS glitch", "harsh_braking": "Harsh braking"}
SEV_COLOR = {"high": [220, 38, 38], "medium": [245, 158, 11], "low": [100, 116, 139]}


@st.cache_resource
def store() -> Store:
    return Store(STORE_PATH)


st.title("📡 Fleet Live Monitor")
st.caption("Real-time anomaly detection on streaming GPS data · Dubai")

types = st.sidebar.multiselect("Alert types", list(TYPE_LABEL), default=list(TYPE_LABEL),
                               format_func=TYPE_LABEL.get)
severities = st.sidebar.multiselect("Severity", ["high", "medium", "low"], default=["high", "medium", "low"])
st.sidebar.caption(f"Store: {STORE_PATH}")


@st.fragment(run_every=2)
def live():
    s = store()
    stats, pos, alerts = s.stats(), s.positions(), s.alerts(1000)
    if pos.empty:
        st.info("Waiting for data… start the stream with `make demo` (no Kafka) or `make up` (Kafka).")
        return
    alerts = alerts[alerts["type"].isin(types) & alerts["severity"].isin(severities)]
    age = time.time() - stats.get("updated_at", 0)

    c = st.columns(6)
    c[0].metric("Vehicles live", int(stats.get("vehicles", len(pos))))
    c[1].metric("Moving", int((pos["speed_kmh"] > 3).sum()))
    c[2].metric("Events processed", f"{int(stats.get('processed', 0)):,}")
    c[3].metric("Throughput", f"{stats.get('events_per_s', 0):,.0f}/s")
    c[4].metric("Alerts", len(alerts))
    c[5].metric("Duplicates removed", f"{int(stats.get('duplicates', 0)):,}")
    if age > 15:
        st.warning(f"No new data for {age:.0f}s. Is the stream running?")

    left, right = st.columns([3, 2])
    with left:
        recent = alerts.head(60).copy()
        recent["color"] = recent["severity"].map(SEV_COLOR)
        recent["label"] = recent["type"].map(TYPE_LABEL) + " · " + recent["vehicle_id"]
        pos = pos.assign(color=[[37, 99, 235] if v > 3 else [148, 163, 184] for v in pos["speed_kmh"]])
        zones = pd.DataFrame([{"name": z, "polygon": [[lon, lat] for lat, lon in p]} for z, p in GEOFENCES.items()])
        layers = [
            pdk.Layer("PolygonLayer", zones, get_polygon="polygon", get_fill_color=[220, 38, 38, 40],
                      get_line_color=[220, 38, 38], line_width_min_pixels=1, pickable=True),
            pdk.Layer("ScatterplotLayer", pos, get_position="[lon, lat]", get_fill_color="color",
                      get_radius=180, pickable=True),
            pdk.Layer("ScatterplotLayer", recent, get_position="[lon, lat]", get_fill_color="color",
                      get_radius=420, opacity=0.35, pickable=True),
        ]
        st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=pdk.ViewState(latitude=25.15, longitude=55.27,
                                 zoom=9.6), tooltip={"text": "{vehicle_id}{name}{label}"}, map_style=None),
                        height=470)
    with right:
        st.subheader("Alerts by type")
        counts = alerts["type"].map(TYPE_LABEL).value_counts()
        st.bar_chart(counts, height=200, horizontal=True)
        st.subheader("Latest alerts")
        feed = alerts.head(12).copy()
        feed["what"] = feed["type"].map(TYPE_LABEL)
        feed["detail"] = feed["details"].map(lambda d: ", ".join(f"{k}={v}" for k, v in json.loads(d).items()))
        feed["time"] = feed["detected_at"].str[11:19]
        st.dataframe(feed[["time", "vehicle_id", "what", "severity", "detail"]], hide_index=True,
                     use_container_width=True, height=300)


live()
