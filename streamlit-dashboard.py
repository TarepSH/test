import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import streamlit as st
import redis
import clickhouse_connect

# ------- Config -------
CH_HOST   = os.getenv("CH_HOST", "localhost")
CH_PORT   = int(os.getenv("CH_PORT", "8123"))
CH_USER   = os.getenv("CH_USER", "ch")
CH_PASS   = os.getenv("CH_PASS", "chpw")
CH_DB     = os.getenv("CH_DB", "analytics")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

# ------- Clients -------
ch = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB)
r  = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

st.set_page_config(page_title="Engagement Dashboard", layout="wide")
st.title("🎛️ Engagement Dashboard (Local)")
st.caption("Postgres → Kafka → Enricher → ClickHouse + Redis")

# Helper to coerce Decimal to float for charts
def d2f(x):
    if isinstance(x, Decimal): return float(x)
    return x

# ------------------ ClickHouse panels ------------------
with st.container():
    st.subheader("Recent enriched events (ClickHouse)")
    limit = st.slider("Rows", 10, 500, 50, 10)
    rows = ch.query(
        f"""
        SELECT event_ts, content_id, user_id, event_type, duration_ms,
               content_type, length_seconds, engagement_seconds, engagement_pct
        FROM engagement_enriched
        ORDER BY event_ts DESC
        LIMIT {limit}
        """
    ).result_rows
    df_recent = pd.DataFrame(rows, columns=["event_ts","content_id","user_id","event_type","duration_ms","content_type","length_seconds","engagement_seconds","engagement_pct"])
    df_recent["engagement_seconds"] = df_recent["engagement_seconds"].map(d2f)
    df_recent["engagement_pct"]     = df_recent["engagement_pct"].map(d2f)
    st.dataframe(df_recent, use_container_width=True, height=300)

with st.container():
    st.subheader("Engagement by content (last 60 min) — ClickHouse")
    rows = ch.query(
        """
        SELECT content_id,
               sumOrNull(engagement_seconds) AS total_eng_s
        FROM engagement_enriched
        WHERE event_ts >= now() - INTERVAL 60 MINUTE
        GROUP BY content_id
        ORDER BY total_eng_s DESC
        LIMIT 20
        """
    ).result_rows
    df_hour = pd.DataFrame(rows, columns=["content_id","total_eng_s"])
    df_hour["total_eng_s"] = df_hour["total_eng_s"].map(d2f).fillna(0)
    st.bar_chart(df_hour.set_index("content_id")["total_eng_s"])

# ------------------ Redis real-time (last 10 minutes) ------------------
st.subheader("Real-time Top N (last 10 minutes) — Redis")
col1, col2 = st.columns(2)
with col1:
    top_n = st.slider("Top N", 3, 20, 10, 1)
with col2:
    window_min = st.slider("Window (minutes)", 5, 30, 10, 1)

def minute_key(dt):
    return "zset:eng:minute:" + dt.strftime("%Y%m%d%H%M")

now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
keys = [minute_key(now - timedelta(minutes=i)) for i in range(window_min)]

# Use ZUNIONSTORE into a short-lived temp key
tmp_key = f"zset:eng:window:{int(datetime.now().timestamp())}"
if keys:
    try:
        r.zunionstore(tmp_key, keys, aggregate="SUM")
        r.expire(tmp_key, 5)
        data = r.zrevrange(tmp_key, 0, top_n-1, withscores=True)
    except Exception as e:
        data = []
else:
    data = []

df_rt = pd.DataFrame(data, columns=["content_id","score_seconds"])
if not df_rt.empty:
    st.bar_chart(df_rt.set_index("content_id")["score_seconds"])
st.dataframe(df_rt, use_container_width=True)
st.caption("Scores are summed engagement_seconds across the last N minute buckets.")
