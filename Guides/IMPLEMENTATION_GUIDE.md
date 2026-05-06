# ⚡ Quick Implementation Guide & Checklist

## 📌 Current Project Status: FOUNDATION PHASE

The project has a **solid foundation** but is **missing core features** for production use. Think of it as:

```
✅ Architecture & Infrastructure: 90% (Kafka, HDFS, Spark, Docker)
✅ Data Ingestion: 80% (Bluesky → Kafka → HDFS)
❌ Core Analytics: 0% (NO sentiment analysis!)
❌ Data Persistence: 0% (only console output)
❌ Visualization: 0% (no dashboard)
❌ Monitoring: 0% (no metrics/alerts)
```

---

## 🎯 Absolute Priorities (Must-Have)

Before anything else, implement these 3 items:

### 1️⃣ Sentiment Analysis Implementation
**Why:** It's the entire point of the project. Currently posts are just being counted, not analyzed.

**Quick Implementation (2-3 days):**
```python
# In spark/stream.py, after df_json definition:

from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType

# Load pre-trained model
def load_sentiment_model():
    # Option: Use transformers pipeline (slow but simple)
    from transformers import pipeline
    return pipeline("sentiment-analysis", 
                   model="distilbert-base-uncased-finetuned-sst-2-english")

# Create UDF
sentiment_model = load_sentiment_model()

def analyze_sentiment(text):
    if not text:
        return "NEUTRAL", 0.5
    result = sentiment_model(text[:512])[0]  # Truncate to 512 chars
    label = result['label'].upper()
    score = result['score']
    # Map to POSITIVE/NEGATIVE/NEUTRAL
    if label == 'POSITIVE':
        return 'POSITIVE', float(score)
    else:
        return 'NEGATIVE', float(score)

sentiment_udf = udf(analyze_sentiment, 
                    StructType([
                        StructField("sentiment", StringType()),
                        StructField("score", DoubleType())
                    ]))

# Apply to stream
df_enriched = df_json.withColumn("sentiment_data", sentiment_udf(col("text")))
               .select("text", "sentiment_data.*")
```

**Alternative (faster but requires setup):**
- Use `TextBlob` (simpler, no deep learning): `pip install textblob`
- Use `VADER` from NLTK (sentiment-specific): `pip install nltk`
- Use external service (AWS Comprehend, Google Cloud NLP)

---

### 2️⃣ Database Persistence
**Why:** Currently, only Spark console output exists. No data is persisted!

**Quick Implementation (2 days):**

**Option A: PostgreSQL (Recommended)**
```yaml
# Add to docker-compose.yml
postgres:
  image: postgres:15-alpine
  container_name: postgres
  environment:
    POSTGRES_DB: bluesky_analytics
    POSTGRES_USER: analytics
    POSTGRES_PASSWORD: password123
  volumes:
    - postgres_data:/var/lib/postgresql/data
  networks:
    - hadoop-net
  ports:
    - "5432:5432"
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U analytics"]
    interval: 10s
    timeout: 5s
    retries: 5
```

```python
# In spark/stream.py, add sink
def write_to_postgres(df, epoch_id):
    df.write \
      .mode("append") \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres:5432/bluesky_analytics") \
      .option("dbtable", "sentiment_results") \
      .option("user", "analytics") \
      .option("password", "password123") \
      .option("driver", "org.postgresql.Driver") \
      .save()

query = df_enriched.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()
```

**Option B: Simple CSV Export (Fastest)**
```python
# Write to mounted volume
query = df_enriched.writeStream \
    .format("csv") \
    .option("path", "/data/output") \
    .option("checkpointLocation", "/data/checkpoint") \
    .outputMode("append") \
    .start()
```

---

### 3️⃣ Basic Dashboard
**Why:** No visualization = stakeholders have no way to see results.

**Quick Implementation (2 days with Streamlit):**

Create `dashboard/app.py`:
```python
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import plotly.express as px

# Database connection
conn = psycopg2.connect(
    dbname="bluesky_analytics",
    user="analytics",
    password="password123",
    host="postgres",
    port=5432
)

st.set_page_config(page_title="Bluesky Sentiment Dashboard", layout="wide")
st.title("🌐 Bluesky Sentiment Analysis Dashboard")

# Real-time sentiment gauge
col1, col2, col3 = st.columns(3)

# Get last 1 hour of data
query = """
SELECT sentiment, COUNT(*) as count 
FROM sentiment_results 
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY sentiment
"""

df = pd.read_sql_query(query, conn)

with col1:
    positive_count = df[df['sentiment'] == 'POSITIVE']['count'].sum()
    st.metric("😊 Positive", positive_count)

with col2:
    negative_count = df[df['sentiment'] == 'NEGATIVE']['count'].sum()
    st.metric("😞 Negative", negative_count)

with col3:
    neutral_count = df[df['sentiment'] == 'NEUTRAL']['count'].sum()
    st.metric("😐 Neutral", neutral_count)

# Sentiment distribution pie chart
fig = px.pie(df, values='count', names='sentiment', title="Sentiment Distribution (Last Hour)")
st.plotly_chart(fig, use_container_width=True)

# Time series chart
query_ts = """
SELECT 
    DATE_TRUNC('minute', created_at) as time_bucket,
    sentiment,
    COUNT(*) as count
FROM sentiment_results
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('minute', created_at), sentiment
ORDER BY time_bucket DESC
"""

df_ts = pd.read_sql_query(query_ts, conn)
fig_ts = px.line(df_ts, x='time_bucket', y='count', color='sentiment', 
                 title="Sentiment Trend (24 Hours)")
st.plotly_chart(fig_ts, use_container_width=True)

conn.close()
```

Add to docker-compose.yml:
```yaml
streamlit-dashboard:
  build: ./dashboard
  container_name: streamlit-dashboard
  ports:
    - "8501:8501"
  networks:
    - hadoop-net
  depends_on:
    - postgres
```

Access at: `http://localhost:8501`

---

## 📋 Phase 1 Checklist (Week 1)

- [ ] **Sentiment Model**
  - [ ] Choose between transformer, TextBlob, VADER, or external API
  - [ ] Update `spark/Dockerfile` with DL dependencies
  - [ ] Implement sentiment UDF
  - [ ] Test with sample Bluesky posts
  - [ ] Benchmark latency (should be <50ms per post)

- [ ] **Database Setup**
  - [ ] Add PostgreSQL to docker-compose.yml
  - [ ] Create schema: `sentiment_results`, `posts`, `aggregations_hourly`
  - [ ] Configure Spark to JDBC PostgreSQL
  - [ ] Test write operations
  - [ ] Verify data appears in DB

- [ ] **Basic Dashboard**
  - [ ] Create Streamlit app with:
    - [ ] Real-time sentiment counts (metric cards)
    - [ ] Sentiment distribution (pie chart)
    - [ ] 24-hour trend (line chart)
    - [ ] Top keywords (list)
  - [ ] Connect to PostgreSQL
  - [ ] Test with sample data
  - [ ] Add to docker-compose.yml

- [ ] **Testing**
  - [ ] Run `docker compose up --build`
  - [ ] Verify Bluesky bridge connects
  - [ ] Check Kafka has messages with `kafka-console-consumer`
  - [ ] Verify sentiment analysis works
  - [ ] Check data appears in PostgreSQL
  - [ ] Access dashboard at localhost:8501
  - [ ] Check sentiment trends update in real-time

---

## 🔍 Debugging Checklist

Before each phase, verify:

```bash
# Check container logs
docker logs bluesky-bridge
docker logs hdfs-collector
docker logs spark-app

# Check Kafka messages
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bluesky.posts \
  --from-beginning --max-messages 5

# Check HDFS contents
curl -X GET "http://localhost:9870/webhdfs/v1/bluesky/raw?op=LISTSTATUS"

# Check PostgreSQL
docker exec postgres psql -U analytics -d bluesky_analytics -c "SELECT COUNT(*) FROM sentiment_results;"

# Check Spark logs
docker logs spark-app | tail -100
```

---

## 🚀 Phase 2 Checklist (Week 2)

After Phase 1 is stable:

- [ ] **Parquet Format Conversion**
  - [ ] Update collector to write Parquet instead of JSONL
  - [ ] Schema:
    ```
    {
      id: UUID,
      text: String,
      created_at: Timestamp,
      sentiment: String,
      sentiment_score: Double
    }
    ```
  - [ ] Update Spark reader to read Parquet
  - [ ] Benchmark compression ratio vs. JSONL

- [ ] **Windowed Analytics**
  - [ ] Add 5-minute sliding window:
    ```python
    .groupBy(
        window(col("created_at"), "5 minutes", "1 minute"),
        col("sentiment")
    ).agg(count("*").alias("count"))
    ```
  - [ ] Write windowed results to PostgreSQL
  - [ ] Add charts in dashboard

- [ ] **Error Handling & Logging**
  - [ ] Add try/except in bridge.py
  - [ ] Add try/except in collector.py
  - [ ] Add logging to all components
  - [ ] Test failure scenarios (stop Kafka, stop HDFS, etc.)

---

## 🎯 Phase 3-8 (Future Sprints)

```
Week 3: Trending keywords + anomaly detection
Week 4: Prometheus + Grafana monitoring
Week 5: REST API for historical queries
Week 6: Advanced NLP (entity extraction, language detection)
Week 7: Kubernetes deployment preparation
Week 8+: Enterprise features (ML retraining, alerting, etc.)
```

---

## 🛠️ Key Configuration Changes

### Bridge (Get more data)
```python
# In bridge/bridge.py
# Replace:
KEYWORDS = ["apple", "iphone"]

# With env-configurable:
KEYWORDS = os.getenv("KEYWORDS", "bitcoin,ethereum,crypto").split(",")
```

### Collector (Better batching)
```python
# In collector/hdfs_collector.py
# Add time-based flushing (not just count-based)
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "30"))  # seconds

# Add logic:
last_flush = time.time()
for msg in consumer:
    if time.time() - last_flush > FLUSH_INTERVAL or len(buffer) >= FLUSH_EVERY:
        flush_to_hdfs(buffer)
```

### Spark (Checkpointing)
```python
# In spark/stream.py
# Add checkpointing for fault tolerance:
query = df_enriched.writeStream \
    .option("checkpointLocation", "/data/spark_checkpoint") \
    .outputMode("append") \
    .start()
```

---

## 📊 Success Metrics (Phase 1)

After implementing Phase 1, you should see:

✅ **Sentiment labels appear in PostgreSQL** (not just text counts)  
✅ **Dashboard shows real-time sentiment gauge**  
✅ **Sentiment accuracy >75%** (validate against labeled dataset)  
✅ **Latency <2 seconds** (Bluesky → Database)  
✅ **Throughput >100 posts/second**  

---

## ⚠️ Common Pitfalls & Solutions

| Problem | Cause | Solution |
|---------|-------|----------|
| Spark job crashes at start | Missing dependencies | Add to spark/requirements.txt or Dockerfile |
| No sentiment results | Model loading fails | Check transformers installation, test locally |
| PostgreSQL connection timeout | Container not ready | Add depends_on with healthcheck |
| Dashboard shows no data | Query returns empty | Check PostgreSQL has data first |
| High latency (>5s) | Sentiment model too slow | Use VADER instead of transformer |
| Out of memory | Spark executor memory too low | Increase SPARK_EXECUTOR_MEMORY |
| Kafka consumer lag increasing | Processing slower than ingestion | Add more Spark executors |

---

## 📞 Quick Links

- **HDFS Web UI:** http://localhost:9870
- **Spark UI:** http://localhost:8081
- **Kafka Dashboard:** (install Kafdrop) http://localhost:19000
- **Dashboard (Phase 1):** http://localhost:8501
- **PostgreSQL:** localhost:5432

---

## 🎬 Next Steps

1. **Today:** Read this document end-to-end
2. **Tomorrow:** Implement sentiment analysis (Phase 1 item 1)
3. **Day 3:** Set up PostgreSQL + write queries (Phase 1 item 2)
4. **Day 4-5:** Build dashboard (Phase 1 item 3)
5. **Day 6:** Integration testing and debugging
6. **Week 2:** Start Phase 2 (Parquet + windowing)

---

## 📚 Resources

- **Sentiment Analysis Models:** https://huggingface.co/nlp/pipeline
- **Spark Streaming Docs:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- **Streamlit Docs:** https://docs.streamlit.io/
- **PostgreSQL JDBC:** https://jdbc.postgresql.org/
- **Docker Compose:** https://docs.docker.com/compose/compose-file/compose-file-v3/

---

Good luck! 🚀
