# 📋 Development Roadmap — Social Sentiment Analysis Platform

## 📊 Current State Analysis

### ✅ What's Implemented
- ✓ Bluesky WebSocket streaming bridge with keyword filtering
- ✓ Kafka message broker (KRaft mode) for reliable streaming
- ✓ HDFS data lake with time-based partitioning (YYYY-MM-DD/HH)
- ✓ Kafka → HDFS collector with buffering and batching
- ✓ Spark Structured Streaming with console output aggregation
- ✓ Full containerized Docker Compose infrastructure
- ✓ Two-node Hadoop cluster with WebHDFS API
- ✓ Spark Master/Worker cluster setup

### ❌ Gaps Identified
1. **No Sentiment Analysis** — Posts are not classified (positive/negative/neutral)
2. **No Data Persistence** — Spark output only prints to console
3. **No Visualization** — No dashboard to view analytics
4. **No Windowed Analytics** — Missing time-window aggregations (5–10 min trends)
5. **No Data Format Optimization** — JSONL not converted to Parquet
6. **Limited Error Handling** — Minimal resilience and monitoring
7. **No Schema Validation** — Input data quality not checked
8. **No Enrichment Pipeline** — Posts not processed for additional metadata
9. **No Alerting System** — No real-time notifications for trending topics
10. **No Unit Tests** — No test coverage for components

---

## 🎯 Phase 1: Core Sentiment Analysis (Foundation)
**Goal:** Implement sentiment classification pipeline  
**Estimated effort:** 1-2 weeks

### 1.1 Add NLP Sentiment Model
- [ ] Choose lightweight model → `transformers` (DistilBERT) or `textblob` (simpler)
- [ ] Add sentiment analysis stage in Spark Streaming pipeline
- [ ] Output schema: `{text, sentiment: POSITIVE/NEGATIVE/NEUTRAL, score: 0.0-1.0}`
- **Files to create/modify:**
  - `spark/sentiment_model.py` — Load and cache model
  - Update `spark/stream.py` — Add UDF for sentiment classification
  - Update `spark/Dockerfile` — Add transformers dependencies

### 1.2 Modify HDFS Output Format
- [ ] Change from JSONL to Parquet (columnar, compressed, schema-aware)
- [ ] Ensure backward compatibility with existing data
- [ ] Update `collector/hdfs_collector.py` to ingest and convert to Parquet
- **Benefits:** Better compression, faster querying, Native Spark support

### 1.3 Add Structured Output Schema
- [ ] Define unified schema for all components
  ```
  {
    id: UUID,
    text: String,
    author: String,
    created_at: Timestamp,
    sentiment: String (POSITIVE|NEGATIVE|NEUTRAL),
    sentiment_score: Double (0.0-1.0),
    confidence: Double,
    keywords: Array[String],
    language: String,
    source: String ("bluesky")
  }
  ```

---

## 🎯 Phase 2: Persistent Output & Multiple Sinks
**Goal:** Store results in multiple formats for different use cases  
**Estimated effort:** 1-2 weeks

### 2.1 Add Database Layer (PostgreSQL)
- [ ] Add PostgreSQL container to docker-compose.yml
- [ ] Create schema for sentiment analytics
- [ ] Implement Spark → PostgreSQL writer (batch every 1–5 minutes)
- [ ] Tables to create:
  ```sql
  posts (id, text, author, created_at, source)
  sentiment_results (post_id, sentiment, score, confidence, computed_at)
  aggregations_hourly (hour, sentiment, count, avg_score)
  ```
- **Why:** Real-time queries, data integrity, transactional support

### 2.2 Add Data Warehouse Format (Parquet Lake)
- [ ] Partition Parquet files by: date, hour, sentiment
  ```
  /warehouse/YYYY/MM/DD/HH/sentiment=POSITIVE/
  /warehouse/YYYY/MM/DD/HH/sentiment=NEGATIVE/
  /warehouse/YYYY/MM/DD/HH/sentiment=NEUTRAL/
  ```
- [ ] Implement Delta Lake format for ACID compliance
  - Add `delta-lake` package to Spark

### 2.3 Add Real-time Output Sinks
- [ ] **Kafka sink:** Re-publish enriched posts with sentiment to new topic
  - `bluesky.posts.enriched`
  - Enables downstream consumers
- [ ] **Redis cache:** Cache trending topics, hourly aggregations
  - Add Redis container to docker-compose
  - 2-hour TTL for aggregations

---

## 🎯 Phase 3: Advanced Analytics & Windowing
**Goal:** Implement temporal aggregations and trending analysis  
**Estimated effort:** 2-3 weeks

### 3.1 Implement Time Windows
- [ ] Add sliding/tumbling windows in Spark Streaming:
  ```python
  .groupBy(
    window(col("created_at"), "10 minutes", "5 minutes"),
    col("sentiment")
  ).agg(count("*").alias("count"))
  ```
- [ ] Calculate moving averages (30-min, 1-hr, 4-hr, daily)
- [ ] Detect sentiment shifts (positive trend vs. negative trend)

### 3.2 Trending Topics Detection
- [ ] Implement TF-IDF or keyword extraction
- [ ] Track keyword frequency over time windows
- [ ] Identify emerging topics (spikes in mention count)
- [ ] Create "trending" table with:
  ```
  keyword, frequency, sentiment_distribution, trend_score
  ```

### 3.3 Add Anomaly Detection
- [ ] Detect unusual sentiment spikes (e.g., sudden negative surge)
- [ ] Use statistical methods (z-score, IQR)
- [ ] Log anomalies for alerting system (Phase 4)

---

## 🎯 Phase 4: Monitoring, Logging & Observability
**Goal:** Add production-grade logging, metrics, and alerting  
**Estimated effort:** 1-2 weeks

### 4.1 Structured Logging
- [ ] Add Python logging to all components (bridge, collector)
- [ ] Log format: JSON for easy parsing
  ```json
  {
    "timestamp": "2026-05-05T10:30:00Z",
    "component": "hdfs-collector",
    "level": "INFO",
    "message": "Flushed 50 posts",
    "posts_count": 50,
    "duration_ms": 123
  }
  ```
- [ ] Ship logs to centralized system (ELK Stack or CloudWatch)

### 4.2 Metrics & Monitoring
- [ ] Add Prometheus metrics:
  - `posts_received_total` (counter)
  - `kafka_lag` (gauge)
  - `spark_batch_duration_seconds` (histogram)
  - `sentiment_distribution` (gauge)
- [ ] Add Prometheus + Grafana containers
- [ ] Create Grafana dashboards for:
  - Throughput per second
  - Error rates
  - Sentiment distribution over time
  - Latency percentiles

### 4.3 Health Checks
- [ ] Add health check endpoints for all services
- [ ] Kafka broker health
- [ ] HDFS NameNode responsiveness
- [ ] Spark job status
- [ ] Database connectivity

---

## 🎯 Phase 5: Real-time Dashboard
**Goal:** Build interactive visualization for streaming analytics  
**Estimated effort:** 2-3 weeks

### 5.1 Choose Dashboard Technology
- [ ] **Option A:** Streamlit (Python, rapid development)
- [ ] **Option B:** React.js (frontend heavy, more flexible)
- [ ] **Option C:** Superset (Data visualization, multi-user)
- **Recommendation:** Start with Streamlit, migrate to React if needed

### 5.2 Dashboard Features
- [ ] **Real-time sentiment gauge** (live percentage breakdown)
- [ ] **Trending topics** (top 10 keywords, updated every 1 min)
- [ ] **Time-series charts**
  - Sentiment over 24 hours
  - Keyword frequency over time
  - Post volume per hour
- [ ] **Heat maps** (sentiment by time)
- [ ] **Statistics panel** (total posts, avg sentiment score)

### 5.3 Data Source for Dashboard
- [ ] Query PostgreSQL for aggregations
- [ ] Cache results in Redis for performance
- [ ] WebSocket support for real-time updates
- [ ] Pagination for large result sets

---

## 🎯 Phase 6: Data Quality & Testing
**Goal:** Ensure reliability, correctness, and data integrity  
**Estimated effort:** 2 weeks

### 6.1 Unit Tests
- [ ] Test sentiment model accuracy on labeled dataset
- [ ] Test Kafka producer/consumer
- [ ] Test HDFS write operations
- [ ] Test Spark transformations
- **Framework:** pytest

### 6.2 Integration Tests
- [ ] End-to-end flow tests
- [ ] Kafka → HDFS pipeline
- [ ] HDFS → Spark pipeline
- [ ] Database write tests
- **Tool:** Docker Compose with test containers

### 6.3 Data Quality Checks
- [ ] Schema validation on ingestion
- [ ] Null/empty field detection
- [ ] Sentiment score range validation (0–1)
- [ ] Duplicate detection (same post, same timestamp)
- [ ] Freshness checks (age of latest data)

### 6.4 Load Testing
- [ ] Simulate high-volume streams (1k+ posts/sec)
- [ ] Measure throughput, latency, resource usage
- [ ] Identify bottlenecks

---

## 🎯 Phase 7: Production Hardening
**Goal:** Prepare for deployment in production environment  
**Estimated effort:** 2-3 weeks

### 7.1 Configuration Management
- [ ] Externalize all configs (environment variables, ConfigMaps)
- [ ] Support multiple environments (dev, staging, prod)
- [ ] Secret management (API keys, database credentials)

### 7.2 Resilience & Fault Tolerance
- [ ] Implement circuit breakers for external API calls
- [ ] Add retry logic with exponential backoff
- [ ] Implement graceful shutdown handlers
- [ ] Add deadletter queue for failed messages

### 7.3 Kubernetes Deployment
- [ ] Convert docker-compose to Kubernetes manifests
- [ ] Create ConfigMaps and Secrets
- [ ] Set resource requests/limits
- [ ] Add HPA (Horizontal Pod Autoscaling)
- [ ] Implement liveness and readiness probes

### 7.4 Security
- [ ] Enable authentication for Kafka
- [ ] Secure HDFS (Kerberos, if needed)
- [ ] Database credentials in secrets
- [ ] Network policies (restrict traffic between pods)
- [ ] Container image scanning

---

## 🎯 Phase 8: Advanced Features
**Goal:** Add enterprise-grade capabilities  
**Estimated effort:** 3+ weeks

### 8.1 Natural Language Processing Enhancements
- [ ] Multi-language support (detect language, translate)
- [ ] Entity extraction (brands, products, people)
- [ ] Emotion classification (anger, joy, surprise, etc.)
- [ ] Sarcasm detection

### 8.2 Machine Learning Pipeline
- [ ] Retrain sentiment model on new data (monthly)
- [ ] A/B test model versions
- [ ] Model versioning and rollback
- [ ] Feature engineering for improved accuracy

### 8.3 Custom Alerts & Notifications
- [ ] Alert rules (e.g., >80% negative sentiment in last 5 min)
- [ ] Webhook notifications (Slack, email, SMS)
- [ ] Alert severity levels
- [ ] On-call escalation

### 8.4 Historical Analytics API
- [ ] REST API for time-range queries
  ```
  GET /api/sentiment?start=2026-05-01&end=2026-05-05&sentiment=NEGATIVE
  GET /api/trending?timeframe=24h&limit=20
  GET /api/aggregations?period=hourly&date=2026-05-05
  ```
- [ ] Support for complex filters
- [ ] CSV/JSON export

### 8.5 Data Retention & Archival
- [ ] Define retention policies (raw: 30 days, aggregated: 1 year)
- [ ] Archive old Parquet files to S3 or cloud storage
- [ ] Implement cold storage strategy
- [ ] GDPR-compliant data deletion

---

## 📈 Implementation Sequence (Recommended)

```
Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7 → Phase 8
 (2w)     (2w)     (3w)     (2w)     (3w)     (2w)     (3w)     (3w+)
 └─ Core pipeline improvements (weeks 1-3)
       └─ Production readiness (weeks 4-7)
             └─ Enterprise features (weeks 8+)
```

---

## 🛠️ Tech Stack Additions (by phase)

| Phase | Technology | Purpose |
|-------|-----------|---------|
| 1 | transformers, textblob | NLP sentiment analysis |
| 2 | PostgreSQL | Relational data store |
| 2 | Redis | In-memory cache |
| 2 | Delta Lake | Data warehouse layer |
| 4 | Prometheus | Metrics collection |
| 4 | Grafana | Metrics visualization |
| 5 | Streamlit or React | Dashboard UI |
| 7 | Kubernetes | Container orchestration |
| 8 | MLflow | ML model management |
| 8 | Feature Store | Feature engineering |

---

## 📊 Architecture After Completion

```
┌─────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                                │
├─────────────────────────────────────────────────────────────────────┤
│  Bluesky Jetstream → Bridge (WebSocket) → Kafka Broker (input)      │
│                                                                       │
├─────────────────────────────────────────────────────────────────────┤
│                    PROCESSING LAYER                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Kafka Input                                                          │
│         ↓                                                              │
│  Spark Structured Streaming                                           │
│         ├→ Sentiment Analysis UDF                                     │
│         ├→ Entity Extraction                                          │
│         ├→ Window Aggregations                                        │
│         └→ Anomaly Detection                                          │
│         ↓                                                              │
│  Multi-Sink Output                                                    │
│         ├→ PostgreSQL (normalized, queryable)                         │
│         ├→ Parquet/Delta Lake (warehouse)                             │
│         ├→ Kafka Output (bluesky.posts.enriched)                      │
│         └→ Redis (cache, trending)                                    │
│                                                                       │
├─────────────────────────────────────────────────────────────────────┤
│                    STORAGE LAYER                                      │
├─────────────────────────────────────────────────────────────────────┤
│  HDFS Data Lake (partitioned, time-based)                            │
│  PostgreSQL (transactional, normalized)                              │
│  Object Storage (S3, for archival)                                   │
│                                                                       │
├─────────────────────────────────────────────────────────────────────┤
│                    CONSUMPTION LAYER                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Streamlit/React Dashboard                                           │
│  REST API (historical queries)                                       │
│  Grafana (operational metrics)                                       │
│  Slack/Email (alerts & notifications)                                │
│                                                                       │
├─────────────────────────────────────────────────────────────────────┤
│                  OBSERVABILITY LAYER                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Prometheus (metrics) → Grafana (dashboards)                         │
│  ELK Stack (logs) or CloudWatch                                      │
│  Health checks & alerting                                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Wins (Start Here if Time-Constrained)

If you want quick, high-impact improvements:

1. **Week 1:** Sentiment analysis (Phase 1.1 + 1.2)
   - Add DistilBERT sentiment classification
   - Switch to Parquet output
   - **Impact:** Core feature complete ✓

2. **Week 2:** Basic persistence (Phase 2.1)
   - Add PostgreSQL
   - Spark → DB writes
   - **Impact:** Data survives restarts ✓

3. **Week 3:** Dashboard (Phase 5.1 + 5.2)
   - Build Streamlit dashboard
   - Real-time sentiment gauge + trending words
   - **Impact:** Stakeholder-visible results ✓

4. **Week 4:** Monitoring (Phase 4.1 + 4.2)
   - Add Prometheus + Grafana
   - Basic metrics
   - **Impact:** Production visibility ✓

---

## 📝 Success Metrics

- **Latency:** E2E latency < 2 seconds (Bluesky → Dashboard)
- **Throughput:** Handle 1k+ posts/second
- **Availability:** 99.5% uptime
- **Data Quality:** <0.1% error rate on sentiment classification
- **Storage:** Parquet compression ratio > 5:1 vs. JSONL

---

## 🔗 Dependencies Between Phases

```
Phase 1 (Sentiment) ──┐
                      ├──→ Phase 2 (Storage)
                      │
Phase 2 (Storage) ────┼──→ Phase 3 (Analytics)
                      │
Phase 3 (Analytics) ──┼──→ Phase 4 (Monitoring)
                      │
Phase 4 (Monitoring) ─┼──→ Phase 5 (Dashboard)
                      │
Phase 5 (Dashboard) ──┼──→ Phase 6 (Testing)
                      │
Phase 6 (Testing) ────┼──→ Phase 7 (Production)
                      │
Phase 7 (Production) ─┴──→ Phase 8 (Advanced)
```

---

## 📞 Next Steps

1. **Validate this roadmap** with stakeholders
2. **Prioritize phases** based on business goals
3. **Allocate team resources** (2-3 developers recommended)
4. **Create detailed JIRA/GitHub issues** for each phase
5. **Set up CI/CD pipeline** for automation
6. **Begin Phase 1** implementation
