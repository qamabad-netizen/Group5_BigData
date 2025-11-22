#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh
from io import BytesIO
from typing import List

st.set_page_config(page_title="Streaming Dashboard", layout="wide")

# -----------------------------
# Sidebar & config
# -----------------------------
def setup_sidebar():
    st.sidebar.header("Config")
    broker = st.sidebar.text_input("Kafka Broker", "localhost:9092")
    topic = st.sidebar.text_input("Kafka Topic", "streaming-data")
    storage = st.sidebar.selectbox("History Source", ["MongoDB"])
    auto = st.sidebar.checkbox("Auto Refresh", True)
    refresh = st.sidebar.slider("Refresh every (sec)", 2, 60, 5)
    buffer = st.sidebar.number_input("Live buffer size (messages)", min_value=100, max_value=5000, value=1000)
    return {"broker": broker, "topic": topic, "storage": storage, "auto": auto, "refresh": refresh, "buffer": int(buffer)}

# -----------------------------
# Helper: safe decode JSON
# -----------------------------
def safe_decode_message(raw: bytes):
    if raw is None or raw == b"":
        return None
    try:
        s = raw.decode("utf-8").strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None

# -----------------------------
# Create or reuse persistent Kafka consumer in session_state
# -----------------------------
def get_or_create_consumer(broker: str, topic: str):
    key = f"k_consumer::{broker}::{topic}"
    if key in st.session_state:
        return st.session_state[key]
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            auto_offset_reset="latest",  # only new messages from when this consumer is created
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
    except Exception as e:
        st.error(f"Kafka connection error: {e}")
        return None
    st.session_state[key] = consumer
    # initialize a message buffer (list of dicts)
    buf_key = f"k_buffer::{broker}::{topic}"
    if buf_key not in st.session_state:
        st.session_state[buf_key] = []
    return consumer

# -----------------------------
# Poll consumer, append to in-memory buffer, return DataFrame
# -----------------------------
def poll_consumer_to_buffer(broker: str, topic: str, buffer_size: int = 1000) -> pd.DataFrame:
    consumer = get_or_create_consumer(broker, topic)
    if consumer is None:
        return pd.DataFrame()

    buf_key = f"k_buffer::{broker}::{topic}"
    buffer: List[dict] = st.session_state.get(buf_key, [])

    # poll new records (non-blocking ~ short wait)
    try:
        records = consumer.poll(timeout_ms=500)
    except Exception as e:
        st.error(f"Error polling Kafka: {e}")
        return pd.DataFrame()

    # records is dict: TopicPartition -> list of ConsumerRecord
    for tp, recs in records.items():
        for rec in recs:
            doc = safe_decode_message(rec.value)
            if not doc:
                continue
            # normalize timestamp
            ts = doc.get("timestamp")
            if isinstance(ts, str) and ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            try:
                ts_parsed = datetime.fromisoformat(ts) if ts else datetime.utcnow()
            except Exception:
                ts_parsed = datetime.utcnow()
            row = {
                "timestamp": ts_parsed,
                "value": float(doc.get("value", 0)),
                "metric_type": doc.get("metric_type", "unknown"),
                "sensor_id": doc.get("sensor_id", "unknown"),
                "location": doc.get("location", "unknown"),
                "unit": doc.get("unit", "")
            }
            buffer.append(row)

    # cap buffer to last buffer_size
    if len(buffer) > buffer_size:
        buffer = buffer[-buffer_size:]
    st.session_state[buf_key] = buffer

    if not buffer:
        return pd.DataFrame()

    df = pd.DataFrame(buffer)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values("timestamp", inplace=True)
    return df

# -----------------------------
# Historical data from Mongo
# -----------------------------
def get_historical_from_mongo(rng: str = "1h", metrics: List[str] = None, locations: List[str] = None):
    client = MongoClient("mongodb://localhost:27017")
    coll = client["streaming_db"]["weather_history"]

    now = datetime.utcnow()
    mapping = {"1h": now - timedelta(hours=1), "24h": now - timedelta(hours=24), "7d": now - timedelta(days=7), "30d": now - timedelta(days=30)}
    start = mapping.get(rng, now - timedelta(hours=1))

    query = {"timestamp": {"$gte": start}}
    if metrics:
        query["metric_type"] = {"$in": metrics}
    if locations:
        query["location"] = {"$in": locations}

    docs = list(coll.find(query, {"_id": 0}).sort("timestamp", 1))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df

# -----------------------------
# Export helpers
# -----------------------------
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def to_excel_bytes(df: pd.DataFrame):
    try:
        from openpyxl import Workbook  # test import
    except Exception:
        return None
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Report")
    return out.getvalue()

# -----------------------------
# UI: Live tab
# -----------------------------
def live_tab(cfg):
    st.subheader("📡 Live Stream (continuous)")

    df_live = poll_consumer_to_buffer(cfg["broker"], cfg["topic"], buffer_size=cfg["buffer"])
    if df_live.empty:
        st.info("No live messages received yet.")
        return

    latest = df_live.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Latest Metric", latest["metric_type"])
    c2.metric("Latest Value", f"{latest['value']} {latest.get('unit','')}")
    c3.metric("Sensor", latest["sensor_id"])

    fig = px.line(df_live, x="timestamp", y="value", color="metric_type", title="Live stream (last {} messages)".format(len(df_live)))
    fig.update_traces(mode="lines+markers")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw live data (most recent first)"):
        st.dataframe(df_live.sort_values("timestamp", ascending=False))

# -----------------------------
# UI: Historical tab
# -----------------------------
def historical_tab(cfg):
    st.subheader("📊 Historical Data")

    rng = st.selectbox("Range", ["1h", "24h", "7d", "30d"])
    metrics = st.multiselect("Metrics", ["temperature", "humidity", "pressure", "co2"], default=[])
    locs = st.text_input("Locations (comma-separated)", value="")
    loc_list = [x.strip() for x in locs.split(",") if x.strip()] or None

    df_hist = get_historical_from_mongo(rng, metrics if metrics else None, loc_list)
    if df_hist.empty:
        st.info("No historical data for the selected filters.")
        return

    st.metric("Records loaded", len(df_hist))
    fig = px.line(df_hist, x="timestamp", y="value", color="metric_type", title="Historical Data")
    fig.update_traces(mode="lines+markers")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw historical data"):
        st.dataframe(df_hist)

    # exports
    csv_bytes = to_csv_bytes(df_hist)
    st.download_button("Download CSV", data=csv_bytes, file_name="historical.csv", mime="text/csv")
    excel_bytes = to_excel_bytes(df_hist)
    if excel_bytes:
        st.download_button("Download XLSX", data=excel_bytes, file_name="historical.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("XLSX export disabled (openpyxl not installed). Install openpyxl to enable it.")

# -----------------------------
# Main
# -----------------------------
def main():
    cfg = setup_sidebar()
    if cfg["auto"]:
        st_autorefresh(interval=cfg["refresh"] * 1000, key="auto_refresh")

    tab_live, tab_hist = st.tabs(["📡 Live", "📊 Historical"])
    with tab_live:
        live_tab(cfg)
    with tab_hist:
        historical_tab(cfg)


if __name__ == "__main__":
    main()
