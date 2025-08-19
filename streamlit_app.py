import time
import pandas as pd
import psycopg2
import streamlit as st


@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host="postgres",
        database="metricsdb",
        user="user",
        password="password",
        port=5432,
    )


def fetch_df(limit=600):
    conn = get_conn()
    df = pd.read_sql(
        f"""
        SELECT ts, cpu, memory, disk, anomaly, score
        FROM system_metrics
        ORDER BY ts DESC
        LIMIT {limit}
        """,
        conn,
    )
    df = df.sort_values("ts")
    return df


st.set_page_config(
    page_title="System Monitoring (Kafka → ML → PG → Streamlit)",
    layout="wide"
)
st.title("🖥️ System Monitoring with Anomaly Detection")

refresh = st.sidebar.selectbox("Refresh (seconds)", [2, 3, 5, 10], index=1)

kpi = st.empty()
charts = st.empty()
table = st.empty()


def highlight_anomalies(row):
    """Highlight rows where anomaly=True."""
    return ["background-color: #ffdddd" if row["anomaly"] else "" for _ in row]


while True:
    df = fetch_df(600)

    if df.empty:
        st.info("Waiting for data…")
        time.sleep(refresh)
        continue

    latest = df.iloc[-1]
    status = "🚨 Anomaly" if latest["anomaly"] else "Normal"

    # KPI Metrics
    with kpi.container():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("CPU %", f"{latest['cpu']:.1f}")
        c2.metric("Memory %", f"{latest['memory']:.1f}")
        c3.metric("Disk %", f"{latest['disk']:.1f}")
        c4.metric("Status", status)

    # Charts
    with charts.container():
        a, b, c = st.columns(3)
        a.subheader("CPU %")
        a.line_chart(df.set_index("ts")[["cpu"]])
        b.subheader("Memory %")
        b.line_chart(df.set_index("ts")[["memory"]])
        c.subheader("Disk %")
        c.line_chart(df.set_index("ts")[["disk"]])

    # Data Table with highlights
    with table.container():
        st.subheader("Recent Samples")
        styled = df.tail(120).style.apply(highlight_anomalies, axis=1)
        st.dataframe(styled, use_container_width=True)

    time.sleep(refresh)
