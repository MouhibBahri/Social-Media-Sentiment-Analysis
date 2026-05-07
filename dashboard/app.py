import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px
from datetime import datetime
import os

# PostgreSQL connection settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "bluesky")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bluesky_postgres_pwd")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sentiment_analytics")

# Map UI label → SQL interval (None = no time filter)
WINDOW_OPTIONS = {
    "Last 15 minutes": "15 minutes",
    "Last 1 hour": "1 hour",
    "Last 6 hours": "6 hours",
    "Last 24 hours": "24 hours",
    "Last 7 days": "7 days",
    "All time": None,
}


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        cursor_factory=RealDictCursor,
    )
    return conn


def _where_window(interval, ts_column="processed_at"):
    """Return a SQL WHERE-clause fragment for the configured time window."""
    if interval is None:
        return "TRUE"
    return f"{ts_column} > NOW() - INTERVAL '{interval}'"


@st.cache_data(ttl=15)
def load_system_status():
    """All-time stats: total rows, sources, last activity. Never filtered."""
    conn = get_connection()
    query = """
        SELECT
            (SELECT COUNT(*) FROM sentiment_posts)                       AS total_posts,
            (SELECT COUNT(DISTINCT source) FROM sentiment_posts)         AS total_sources,
            (SELECT COUNT(DISTINCT author) FROM sentiment_posts)         AS total_authors,
            (SELECT MAX(processed_at) FROM sentiment_posts)              AS last_processed_at,
            (SELECT COUNT(*) FROM sentiment_aggregate)                   AS total_aggregates
    """
    return pd.read_sql_query(query, conn)


@st.cache_data(ttl=15)
def load_sources():
    conn = get_connection()
    query = """
        SELECT COALESCE(source, 'unknown') AS source, COUNT(*) AS posts
        FROM sentiment_posts
        GROUP BY source
        ORDER BY posts DESC
    """
    df = pd.read_sql_query(query, conn)
    return df["source"].dropna().tolist() if not df.empty else []


@st.cache_data(ttl=15)
def load_sentiment_counts(selected_sources, interval):
    if not selected_sources:
        return pd.DataFrame(columns=["source", "sentiment", "count"])
    conn = get_connection()
    query = f"""
        SELECT source, sentiment, COUNT(*) AS count
        FROM sentiment_posts
        WHERE {_where_window(interval)}
          AND source = ANY(%s)
        GROUP BY source, sentiment
        ORDER BY source, sentiment
    """
    return pd.read_sql_query(query, conn, params=(selected_sources,))


@st.cache_data(ttl=15)
def load_sentiment_trend(selected_sources, interval):
    if not selected_sources:
        return pd.DataFrame(columns=["time_bucket", "source", "sentiment", "count"])
    conn = get_connection()
    bucket = "minute" if interval in ("15 minutes", "1 hour") else "hour"
    query = f"""
        SELECT
            DATE_TRUNC('{bucket}', processed_at) AS time_bucket,
            source,
            sentiment,
            COUNT(*) AS count
        FROM sentiment_posts
        WHERE {_where_window(interval)}
          AND source = ANY(%s)
        GROUP BY 1, source, sentiment
        ORDER BY time_bucket ASC
    """
    return pd.read_sql_query(query, conn, params=(selected_sources,))


@st.cache_data(ttl=30)
def load_top_authors(selected_sources, interval):
    if not selected_sources:
        return pd.DataFrame(columns=["author", "source", "count"])
    conn = get_connection()
    query = f"""
        SELECT author, source, COUNT(*) AS count
        FROM sentiment_posts
        WHERE {_where_window(interval)}
          AND source = ANY(%s)
        GROUP BY author, source
        ORDER BY count DESC
        LIMIT 10
    """
    return pd.read_sql_query(query, conn, params=(selected_sources,))


@st.cache_data(ttl=15)
def load_recent_posts(selected_sources, limit=25):
    if not selected_sources:
        return pd.DataFrame()
    conn = get_connection()
    query = """
        SELECT id, text, author, source, sentiment, sentiment_score,
               confidence, created_at, processed_at
        FROM sentiment_posts
        WHERE source = ANY(%s)
        ORDER BY processed_at DESC
        LIMIT %s
    """
    return pd.read_sql_query(query, conn, params=(selected_sources, limit))

st.set_page_config(page_title="Social Media Sentiment Dashboard", layout="wide")
st.title("🌐 Social Media Sentiment Analysis Dashboard")

# Sidebar controls
with st.sidebar:
    st.header("Dashboard Controls")
    refresh = st.button("🔄 Refresh now")
    auto_refresh = st.checkbox("Auto-refresh (15s)", value=True)

    st.markdown("---")
    window_label = st.selectbox(
        "Time window",
        options=list(WINDOW_OPTIONS.keys()),
        index=3,  # Last 24 hours
    )
    interval = WINDOW_OPTIONS[window_label]

    sources = load_sources()
    if not sources:
        st.warning(
            "No data in `sentiment_posts` yet. Make sure the bridges, Kafka and "
            "Spark streaming job are running. Metrics in Grafana (port 3000) "
            "will tell you whether messages are flowing."
        )
        selected_sources = []
    else:
        selected_sources = st.multiselect(
            "Select sources", options=sources, default=sources
        )
        if not selected_sources:
            selected_sources = sources

    st.markdown("---")
    st.markdown("**Links**")
    st.markdown("- [Grafana](http://localhost:3000)  *(admin / admin)*")
    st.markdown("- [Prometheus](http://localhost:9090)")
    st.markdown("- [Spark UI](http://localhost:8081)")
    st.markdown("- [HDFS NameNode](http://localhost:9870)")
    st.write(f"UI rendered: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    if refresh:
        st.cache_data.clear()

# System status (always visible, never time-filtered)
status_df = load_system_status()
if not status_df.empty:
    s = status_df.iloc[0]
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("📦 Total posts", int(s["total_posts"] or 0))
    s2.metric("🔌 Sources", int(s["total_sources"] or 0))
    s3.metric("👤 Authors", int(s["total_authors"] or 0))
    s4.metric("Σ Aggregate rows", int(s["total_aggregates"] or 0))
    last_proc = s["last_processed_at"]
    s5.metric(
        "🕒 Last batch (UTC)",
        last_proc.strftime("%H:%M:%S") if last_proc else "—",
    )

st.markdown("---")
st.subheader(f"Sentiment Metrics — {window_label}")

# Load window-filtered data
counts_df = load_sentiment_counts(selected_sources, interval)
trend_df = load_sentiment_trend(selected_sources, interval)
top_authors_df = load_top_authors(selected_sources, interval)
recent_posts_df = load_recent_posts(selected_sources)

col1, col2, col3, col4 = st.columns(4)
positive = int(counts_df[counts_df["sentiment"] == "POSITIVE"]["count"].sum()) if not counts_df.empty else 0
negative = int(counts_df[counts_df["sentiment"] == "NEGATIVE"]["count"].sum()) if not counts_df.empty else 0
neutral = int(counts_df[counts_df["sentiment"] == "NEUTRAL"]["count"].sum()) if not counts_df.empty else 0
total = positive + negative + neutral

col1.metric("Total in window", total)
col2.metric("😊 Positive", positive)
col3.metric("😞 Negative", negative)
col4.metric("😐 Neutral", neutral)

st.markdown("---")
row1_col1, row1_col2 = st.columns([2, 1])

with row1_col1:
    st.subheader("Sentiment by source")
    if counts_df.empty:
        st.info(f"No sentiment records in selected window ({window_label}).")
    else:
        fig = px.bar(
            counts_df,
            x="source", y="count", color="sentiment",
            barmode="group", title=f"Sentiment counts — {window_label}",
        )
        st.plotly_chart(fig, use_container_width=True)
        agg = counts_df.groupby("sentiment", as_index=False)["count"].sum()
        fig_pie = px.pie(agg, values="count", names="sentiment", title="Share of sentiment")
        st.plotly_chart(fig_pie, use_container_width=True)

with row1_col2:
    st.subheader("Top authors")
    if top_authors_df.empty:
        st.info(f"No author activity in window ({window_label}).")
    else:
        authors_df = top_authors_df.rename(columns={"author": "Author", "count": "Posts"})
        st.bar_chart(authors_df.set_index("Author")["Posts"])

st.markdown("---")
st.subheader("Sentiment trend")
if trend_df.empty:
    st.info("No trend data in selected window.")
else:
    fig_ts = px.line(
        trend_df, x="time_bucket", y="count",
        color="sentiment", line_dash="source", markers=True,
    )
    st.plotly_chart(fig_ts, use_container_width=True)

st.markdown("---")
st.subheader("Latest posts")
if recent_posts_df.empty:
    st.info("No recent posts available.")
else:
    st.dataframe(recent_posts_df, use_container_width=True)

# Cheap auto-refresh: rerun the script every 15s when enabled
if auto_refresh:
    import time
    time.sleep(15)
    st.cache_data.clear()
    st.rerun()
