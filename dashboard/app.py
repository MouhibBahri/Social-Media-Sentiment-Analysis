import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px
from datetime import datetime

# PostgreSQL connection settings
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_USER = "bluesky"
POSTGRES_PASSWORD = "bluesky_postgres_pwd"
POSTGRES_DB = "sentiment_analytics"

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

@st.cache_data(ttl=15)
def load_sources():
    conn = get_connection()
    query = """
        SELECT DISTINCT COALESCE(source, 'unknown') AS source
        FROM sentiment_posts
        ORDER BY source
    """
    df = pd.read_sql_query(query, conn)
    return df["source"].dropna().tolist() if not df.empty else ["unknown"]

@st.cache_data(ttl=15)
def load_sentiment_counts(selected_sources):
    if not selected_sources:
        return pd.DataFrame(columns=["source", "sentiment", "count"])
    conn = get_connection()
    query = """
        SELECT source, sentiment, COUNT(*) AS count
        FROM sentiment_posts
        WHERE created_at > NOW() - INTERVAL '1 hour'
          AND source = ANY(%s)
        GROUP BY source, sentiment
        ORDER BY source, sentiment
    """
    df = pd.read_sql_query(query, conn, params=(selected_sources,))
    return df

@st.cache_data(ttl=15)
def load_sentiment_trend(selected_sources):
    if not selected_sources:
        return pd.DataFrame(columns=["time_bucket", "source", "sentiment", "count"])
    conn = get_connection()
    query = """
        SELECT
            DATE_TRUNC('hour', created_at) AS time_bucket,
            source,
            sentiment,
            COUNT(*) AS count
        FROM sentiment_posts
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND source = ANY(%s)
        GROUP BY DATE_TRUNC('hour', created_at), source, sentiment
        ORDER BY time_bucket ASC
    """
    df = pd.read_sql_query(query, conn, params=(selected_sources,))
    return df

@st.cache_data(ttl=30)
def load_top_authors(selected_sources):
    if not selected_sources:
        return pd.DataFrame(columns=["author", "source", "count"])
    conn = get_connection()
    query = """
        SELECT author, source, COUNT(*) AS count
        FROM sentiment_posts
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND source = ANY(%s)
        GROUP BY author, source
        ORDER BY count DESC
        LIMIT 10
    """
    df = pd.read_sql_query(query, conn, params=(selected_sources,))
    return df

@st.cache_data(ttl=15)
def load_recent_posts(selected_sources):
    if not selected_sources:
        return pd.DataFrame(columns=["id", "text", "author", "source", "sentiment", "sentiment_score", "confidence", "created_at"])
    conn = get_connection()
    query = """
        SELECT id, text, author, source, sentiment, sentiment_score, confidence, created_at
        FROM sentiment_posts
        WHERE source = ANY(%s)
        ORDER BY processed_at DESC
        LIMIT 20
    """
    df = pd.read_sql_query(query, conn, params=(selected_sources,))
    return df

st.set_page_config(page_title="Social Media Sentiment Dashboard", layout="wide")
st.title("🌐 Social Media Sentiment Analysis Dashboard")

with st.sidebar:
    st.header("Dashboard Controls")
    refresh = st.button("Refresh Data")
    st.markdown("---")
    sources = load_sources()
    selected_sources = st.multiselect("Select sources", options=sources, default=sources)
    if not selected_sources:
        selected_sources = sources

    st.markdown("---")
    st.write("**Database:** sentiment_analytics")
    st.write("**Table:** sentiment_posts")
    st.write("**Sources:** " + ", ".join(selected_sources))
    st.write(f"Last update: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    if refresh:
        st.cache_data.clear()
        st.cache_resource.clear()

counts_df = load_sentiment_counts(selected_sources)
trend_df = load_sentiment_trend(selected_sources)
top_authors_df = load_top_authors(selected_sources)
recent_posts_df = load_recent_posts(selected_sources)

col1, col2, col3 = st.columns(3)

positive = int(counts_df[counts_df['sentiment'] == 'POSITIVE']['count'].sum()) if not counts_df.empty else 0
negative = int(counts_df[counts_df['sentiment'] == 'NEGATIVE']['count'].sum()) if not counts_df.empty else 0
neutral = int(counts_df[counts_df['sentiment'] == 'NEUTRAL']['count'].sum()) if not counts_df.empty else 0

total = positive + negative + neutral

with col1:
    st.metric("😊 Positive", positive)
with col2:
    st.metric("😞 Negative", negative)
with col3:
    st.metric("😐 Neutral", neutral)

st.markdown("---")

row1_col1, row1_col2 = st.columns([2, 1])

with row1_col1:
    st.subheader("Sentiment Distribution (Last Hour)")
    if counts_df.empty:
        st.info("No sentiment records found for the last hour.")
    else:
        fig = px.bar(counts_df, x='source', y='count', color='sentiment', barmode='group', title='Last Hour Sentiment by Source')
        st.plotly_chart(fig, width='stretch')

        fig_pie = px.pie(counts_df.groupby('sentiment', as_index=False).sum(), values='count', names='sentiment', title='Last Hour Sentiment Share')
        st.plotly_chart(fig_pie, width='stretch')

with row1_col2:
    st.subheader("Top Authors (Last 24h)")
    if top_authors_df.empty or 'author' not in top_authors_df.columns:
        st.info("No author activity found in the last 24 hours.")
    else:
        authors_df = top_authors_df.rename(columns={'author': 'Author', 'count': 'Posts'})
        if 'Author' in authors_df.columns:
            st.bar_chart(authors_df.set_index('Author'), width='stretch')
        else:
            st.info("No author activity found in the last 24 hours.")

st.markdown("---")

st.subheader("Sentiment Trend (24 Hours)")
if trend_df.empty:
    st.info("No sentiment trend data available.")
else:
    fig_ts = px.line(trend_df, x='time_bucket', y='count', color='sentiment', markers=True)
    st.plotly_chart(fig_ts, width='stretch')

st.markdown("---")

st.subheader("Latest Posts")
if recent_posts_df.empty:
    st.info("No recent posts available.")
else:
    st.dataframe(recent_posts_df, width='stretch')
