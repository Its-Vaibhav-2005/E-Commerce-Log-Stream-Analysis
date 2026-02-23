import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Kafka Events Dashboard", layout="wide")
DB_URI = "postgresql+psycopg2://postgres:1234@localhost:5432/ecom_kafka"

@st.cache_resource
def GetEngine():
    return create_engine(DB_URI)

def FetchData(query_text, params):
    """Executes a query and returns a DataFrame safely."""
    engine = GetEngine()
    with engine.connect() as conn:
        return pd.read_sql(text(query_text), conn, params=params)

st.title("📊 Real-Time Kafka Events Monitor")

# Sidebar controls stay outside the refresh loop
refreshRate = st.sidebar.slider("Refresh interval (seconds)", 1, 10, 3)
windowMinutes = st.sidebar.slider("Time window (minutes)", 1, 60, 5)

# Initialize Session State for our in-memory data store
if "live_data" not in st.session_state:
    st.session_state.live_data = pd.DataFrame()
    st.session_state.last_fetch_ts = None

# 1. Component Isolation: Only this specific function will rerun
@st.fragment(run_every=f"{refreshRate}s")
def StreamDashboard():
    now = pd.Timestamp.now()
    
    # 2. Delta Fetching Strategy (DBMS Optimization)
    if st.session_state.last_fetch_ts is None:
        # Initial Load: Get the full historical window once
        query = """
            SELECT * FROM kafka_events_silver
            WHERE kafka_ingest_ts >= NOW() - (INTERVAL '1 minute' * :mins)
        """
        new_data = FetchData(query, {"mins": windowMinutes})
    else:
        # Delta Load: Only fetch rows that arrived AFTER our last check
        query = """
            SELECT * FROM kafka_events_silver
            WHERE kafka_ingest_ts > :last_ts
        """
        new_data = FetchData(query, {"last_ts": st.session_state.last_fetch_ts})
    
    # 3. State Management & Pruning (LLD Optimization)
    if not new_data.empty:
        new_data['kafka_ingest_ts'] = pd.to_datetime(new_data['kafka_ingest_ts'])
        # Append new delta to our existing in-memory dataframe
        st.session_state.live_data = pd.concat([st.session_state.live_data, new_data])
        # Update the high-water mark timestamp
        st.session_state.last_fetch_ts = new_data['kafka_ingest_ts'].max()
        
    # Prune rows that fall outside the user's requested rolling window to prevent memory leaks
    cutoff_time = now - pd.Timedelta(minutes=windowMinutes)
    df = st.session_state.live_data
    if not df.empty:
        df = df[df['kafka_ingest_ts'] >= cutoff_time]
        st.session_state.live_data = df

    # 4. Render UI
    if df.empty:
        st.warning("No recent events found in the specified time window.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events", len(df))
    col2.metric("Unique Customers", df["customer_id"].nunique())
    col3.metric("Total Revenue", f"${df['amount'].sum():,.2f}")

    st.divider()
    
    st.subheader("Events by Type")
    st.bar_chart(df["event_type"].value_counts())

    st.subheader("Revenue Over Time")
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    revenue_series = df.set_index("event_timestamp")["amount"].resample("1Min").sum()
    st.line_chart(revenue_series)

    st.subheader("Latest Events")
    # Sort descending so newest events are always at the top of the table
    st.dataframe(df.sort_values("kafka_ingest_ts", ascending=False).head(20), width="stretch")

# Execute the isolated fragment
StreamDashboard()