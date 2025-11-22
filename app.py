# Streaming Data Dashboard - Completed Version (Based on Template)
# This version keeps the structure, follows the TODO template, and implements the required fixes.

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------------------
# SIDEBAR CONFIGURATION (TODO COMPLETED)
# ---------------------------------------------------------------------
def setup_sidebar():
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", "localhost:9092"
    )
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", "streaming-data"
    )

    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type", ["MongoDB", "HDFS"]
    )

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

# ---------------------------------------------------------------------
# REAL KAFKA CONSUMER (TODO COMPLETED)
# ---------------------------------------------------------------------
def consume_kafka_data(config):
    broker = config["kafka_broker"]
    topic = config["kafka_topic"]

    cache_key = f"kafka_consumer_{broker}_{topic}"
    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                topic,
                bootstrap_servers=[broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1500
            )
        except Exception as e:
            st.error(f"Kafka connection failed: {e}")
            return pd.DataFrame()

    consumer = st.session_state[cache_key]
    messages = []

    try:
        msg_pack = consumer.poll(timeout_ms=800)
        for tp, batch in msg_pack.items():
            for message in batch:
                data = message.value
                if all(k in data for k in ["timestamp", "value", "metric_type", "sensor_id"]):
                    ts = data["timestamp"]
                    if ts.endswith("Z"):
                        ts = ts[:-1] + "+00:00"
                    try:
                        t_parsed = datetime.fromisoformat(ts)
                    except:
                        t_parsed = datetime.utcnow()

                    messages.append({
                        "timestamp": t_parsed,
                        "value": float(data["value"]),
                        "metric_type": data["metric_type"],
                        "sensor_id": data["sensor_id"]
                    })
    except Exception as e:
        st.error(f"Kafka error: {e}")
        return pd.DataFrame()

    if messages:
        return pd.DataFrame(messages)
    return pd.DataFrame()

# ---------------------------------------------------------------------
# HISTORICAL DATA (TODO COMPLETED)
# FIX: Returns ALL metrics, removes filtering issues
# ---------------------------------------------------------------------
def query_historical_data():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["streaming_db"]
        coll = db["weather_history"]

        cursor = coll.find({}, {"_id": 0})
        data = list(cursor)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df

    except Exception as e:
        st.error(f"MongoDB Error: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------
# REAL-TIME VIEW
# ---------------------------------------------------------------------
def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Streaming Dashboard")

    with st.spinner("Fetching real-time data from Kafka..."):
        df = consume_kafka_data(config)

    if df.empty:
        st.warning("No live data available.")
        return

    st.subheader("Real-time Trend")
    fig = px.line(df, x="timestamp", y="value", color="metric_type",
                  title="Live Stream", template="plotly_white")
    fig.update_traces(mode='lines+markers')
    fig.update_traces(mode='lines+markers')
    fig.update_traces(mode='lines+markers')
    st.plotly_chart(fig, width='stretch')

    with st.expander("Raw Data"):
        st.dataframe(df.sort_values("timestamp", ascending=False), width='stretch')

# ---------------------------------------------------------------------
# HISTORICAL VIEW
# ---------------------------------------------------------------------
def display_historical_view(config):
    st.header("📊 Historical Data Analysis")

    df = query_historical_data()
    if df.empty:
        st.warning("No historical data found.")
        return

    st.subheader("Historical Trend (All Metrics)")
    fig = px.line(df, x="timestamp", y="value", color="metric_type",
                  title="Historical Data", template="plotly_white")
    fig.update_traces(mode='lines+markers')
    fig.update_traces(mode='lines+markers')
    st.plotly_chart(fig, width='stretch')

    with st.expander("Raw Historical Data"):
        st.dataframe(df, width='stretch')

# ---------------------------------------------------------------------
# MAIN APP
# ---------------------------------------------------------------------
def main():
    st.title("🚀 Streaming Data Dashboard")

    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")
    auto = st.sidebar.checkbox("Enable Auto Refresh", True)

    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)", 5, 30, 10
    )

    if auto:
        st_autorefresh(interval=refresh_interval * 1000, key="refresh")

    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])

    with tab1:
        display_real_time_view(config, refresh_interval)

    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()
