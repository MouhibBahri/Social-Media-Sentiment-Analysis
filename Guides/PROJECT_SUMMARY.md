# 🎯 Project Summary & Visual Architecture Guide

## 📌 Executive Summary

**Project:** Real-time Bluesky Social Media Sentiment Analysis Pipeline  
**Status:** Foundation phase (Infrastructure 90%, Features 10%)  
**Architecture Type:** Lambda (Batch + Real-time)  
**Tech Stack:** Python, Kafka, Hadoop HDFS, Apache Spark, Docker  

### Current Capabilities
✅ Real-time data ingestion from Bluesky  
✅ Event streaming via Kafka  
✅ Distributed storage via HDFS  
✅ Stream processing via Spark Structured Streaming  

### Missing Capabilities
❌ **Sentiment Analysis** — No classification of posts  
❌ **Persistence** — Only console output, no database  
❌ **Visualization** — No dashboard or UI  
❌ **Analytics** — No trending topics, time windows  
❌ **Monitoring** — No metrics, alerts, observability  

**Priority:** Build sentiment analysis + database persistence ASAP

---

## 🏗️ Current Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA INGESTION                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Bluesky Jetstream                                                │
│  (wss://jetstream2.us-east)                                       │
│           │                                                        │
│           │ WebSocket                                              │
│           ↓                                                        │
│  ┌─────────────────────┐                                           │
│  │  Bluesky Bridge     │  bridge.py                                │
│  │  (Python)           │  - Filter by keywords                    │
│  │  TCP Server: 9999   │  - JSON serialization                    │
│  └────────┬────────────┘                                           │
│           │                                                        │
│           │ Raw JSON                                               │
│           ↓                                                        │
│  ┌─────────────────────┐                                           │
│  │  Kafka Broker       │  KRaft Mode                              │
│  │  Port: 9092         │  Topic: bluesky.posts                    │
│  │  3 partitions       │  1x replication                          │
│  └────────┬────────────┘                                           │
│           │                                                        │
│           │ Messages                                               │
│           ↓                                                        │
│  ┌─────────────────────┐                                           │
│  │  HDFS Collector     │  hdfs_collector.py                       │
│  │  (Python)           │  - Buffer: 50 posts                      │
│  │  - Kafka Consumer   │  - Flush: ~100ms or 50 posts            │
│  │  - WebHDFS Writer   │  - Format: JSONL ⚠️ (upgrade to Parquet) │
│  └────────┬────────────┘                                           │
│           │                                                        │
│           │ JSONL Write                                            │
│           ↓                                                        │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  HDFS Data Lake                                          │    │
│  │  NameNode: namenode:8020                                 │    │
│  │  DataNodes: datanode1, datanode2 (2x replication)        │    │
│  │  WebHDFS: http://localhost:9870                          │    │
│  │  Partitioning: /bluesky/raw/YYYY-MM-DD/HH/posts_*.jsonl  │    │
│  └──────────┬───────────────────────────────────────────────┘    │
│             │                                                      │
└─────────────┼──────────────────────────────────────────────────────┘
              │
              │
┌─────────────┼──────────────────────────────────────────────────────┐
│             │              STREAM PROCESSING                        │
│             │                                                       │
│             ↓                                                       │
│  ┌──────────────────────────────────────────────────┐              │
│  │  Apache Spark Structured Streaming               │              │
│  │  Master: spark://spark-master:7077               │              │
│  │  Worker: 1x spark-worker                         │              │
│  │                                                   │              │
│  │  ✅ Kafka consumer (bluesky.posts topic)         │              │
│  │  ❌ NO sentiment analysis                        │              │
│  │  ❌ ONLY console output (groupBy.count)          │              │
│  │  ❌ NO database write                            │              │
│  │  ❌ NO windowing/aggregation                     │              │
│  │  ❌ NO checkpointing                             │              │
│  └──────────┬───────────────────────────────────────┘              │
│             │                                                       │
│             │ Console Output                                        │
│             ↓                                                       │
│  ┌──────────────────────────────────────────────────┐              │
│  │  Docker Logs (Console)                           │              │
│  │  ❌ NOT PERSISTENT                               │              │
│  │  ❌ NO REAL ANALYTICS                            │              │
│  └──────────────────────────────────────────────────┘              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         MONITORING                               │
├─────────────────────────────────────────────────────────────────┤
│  ❌ No Prometheus metrics                                         │
│  ❌ No Grafana dashboards                                         │
│  ❌ No alerting system                                            │
│  ❌ Only docker logs available                                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Target Architecture (After All Phases)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│  Bluesky Jetstream → Bridge (Configurable Keywords) → Kafka         │
│                                                                      │
│  ✅ Async I/O, reconnection logic                                   │
│  ✅ Schema validation                                               │
│  ✅ Prometheus metrics                                              │
│  ✅ Structured logging (JSON)                                       │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ (Kafka)
┌─────────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Apache Spark Structured Streaming                                   │
│         ↓                                                           │
│    Schema Inference                                                  │
│         ↓                                                           │
│    NLP Enrichment:                                                   │
│      - Sentiment Analysis (transformer/VADER) ← Phase 1             │
│      - Emotion Classification ← Phase 8                              │
│      - Entity Extraction ← Phase 8                                   │
│      - Language Detection ← Phase 8                                  │
│         ↓                                                           │
│    Aggregations & Windows:                                           │
│      - 5-min sliding windows ← Phase 3                              │
│      - Hourly aggregations (count, avg sentiment) ← Phase 3         │
│      - Trending topics detection ← Phase 3                           │
│      - Anomaly detection (z-score) ← Phase 3                         │
│         ↓                                                           │
│    Multi-Sink Output:                                                │
│      ├→ PostgreSQL (normalized, queryable) ← Phase 2               │
│      ├→ Parquet/Delta Lake (warehouse) ← Phase 2                   │
│      ├→ Kafka (bluesky.posts.enriched) ← Phase 2                   │
│      ├→ Redis (caching, TTL) ← Phase 3                             │
│      └→ HDFS (raw + processed) ← Phase 1                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                ↓ (Multiple sinks)
┌─────────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ├─ PostgreSQL (Primary):                                            │
│  │  ├─ posts (id, text, author, created_at, source)                │
│  │  ├─ sentiment_results (post_id, sentiment, score, confidence)    │
│  │  ├─ aggregations_hourly (hour, sentiment, count, avg_score)     │
│  │  └─ trending_topics (keyword, frequency, trend_score)            │
│  │                                                                   │
│  ├─ Parquet/Delta Lake (Data Warehouse):                             │
│  │  └─ /warehouse/YYYY/MM/DD/HH/sentiment={POSITIVE|NEGATIVE}/     │
│  │                                                                   │
│  ├─ Redis Cache:                                                     │
│  │  ├─ trending_topics:list → JSON array                            │
│  │  ├─ sentiment_hourly:YYYY-MM-DD-HH → aggregation metrics        │
│  │  └─ TTL: 2 hours                                                 │
│  │                                                                   │
│  └─ HDFS Data Lake:                                                  │
│     ├─ /bluesky/raw/ (partition: YYYY-MM-DD/HH) → JSONL            │
│     ├─ /bluesky/processed/ → Parquet (enriched with sentiment)     │
│     └─ Retention: 30 days (raw), 1 year (aggregated)                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                ↓ (Query/API/Cache layer)
┌─────────────────────────────────────────────────────────────────────┐
│                      CONSUMPTION LAYER                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ├─ Interactive Dashboard (Phase 5):                                 │
│  │  ├─ Streamlit/React UI                                           │
│  │  ├─ Real-time sentiment gauge                                    │
│  │  ├─ Trending topics (top 20)                                     │
│  │  ├─ Time-series charts (24h, 7d, 30d)                            │
│  │  ├─ Sentiment distribution (pie)                                 │
│  │  └─ Post volume per hour (bar)                                   │
│  │                                                                   │
│  ├─ REST API (Phase 8):                                              │
│  │  ├─ GET /api/sentiment?start=X&end=Y&sentiment=POSITIVE         │
│  │  ├─ GET /api/trending?timeframe=24h&limit=20                    │
│  │  ├─ GET /api/aggregations?period=hourly&date=2026-05-05         │
│  │  └─ CSV/JSON export                                              │
│  │                                                                   │
│  ├─ Alerts & Notifications (Phase 8):                                │
│  │  ├─ Slack: Sentiment anomalies, trend spikes                     │
│  │  ├─ Email: Daily digests                                         │
│  │  ├─ SMS: Critical alerts (P0)                                    │
│  │  └─ Webhook: Custom integrations                                 │
│  │                                                                   │
│  └─ Internal Tools:                                                  │
│     ├─ ML Model Management (MLflow) ← Phase 8                       │
│     ├─ Data Profiling (Great Expectations)                          │
│     └─ Feature Store (Feast)                                         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      OBSERVABILITY LAYER                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ├─ Metrics (Phase 4):                                               │
│  │  ├─ Prometheus: posts_received, kafka_lag, sentiment_distribution
│  │  ├─ Grafana dashboards: throughput, latency, error rate         │
│  │  └─ Custom metrics: sentiment accuracy, model latency            │
│  │                                                                   │
│  ├─ Logs (Phase 4):                                                  │
│  │  ├─ ELK Stack or CloudWatch                                      │
│  │  ├─ JSON structured logs from all components                     │
│  │  └─ Correlation IDs for tracing                                  │
│  │                                                                   │
│  ├─ Traces (Phase 4):                                                │
│  │  ├─ Jaeger distributed tracing                                   │
│  │  ├─ Span: Bluesky → Kafka → HDFS → Spark → Database             │
│  │  └─ P99 latency tracking                                         │
│  │                                                                   │
│  └─ Health (Phase 4):                                                │
│     ├─ Readiness probes: Can handle requests?                       │
│     ├─ Liveness probes: Is process alive?                           │
│     ├─ Startup probes: Has initialization completed?                │
│     └─ Custom checks: Database connectivity, Kafka lag              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                    INFRASTRUCTURE (Phase 7)                          │
├─────────────────────────────────────────────────────────────────────┤
│  ├─ Kubernetes Deployment                                            │
│  │  ├─ Namespace per environment (dev, staging, prod)               │
│  │  ├─ ConfigMaps: Configuration parameters                         │
│  │  ├─ Secrets: API keys, database passwords                        │
│  │  ├─ Persistent Volumes: HDFS, PostgreSQL, Kafka                  │
│  │  ├─ StatefulSets: Kafka, PostgreSQL (order + state)              │
│  │  ├─ Deployments: Spark, services (stateless)                     │
│  │  ├─ Service: Internal DNS, LoadBalancing                         │
│  │  └─ Ingress: External traffic routing                            │
│  │                                                                   │
│  ├─ Autoscaling (HPA):                                               │
│  │  ├─ Spark workers: Auto-scale based on Kafka lag                │
│  │  ├─ API servers: Auto-scale based on request rate               │
│  │  └─ Min/Max pods configured per environment                      │
│  │                                                                   │
│  └─ Helm Charts:                                                     │
│     ├─ Package management                                            │
│     ├─ Environment-specific values (dev/staging/prod)               │
│     └─ Version control + rollback capability                        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Data Flow Timeline

### Current State (Week 0)
```
Bluesky Post
   │ (200ms) ── WebSocket ──→ Bridge (TCP write)
   │ (100ms) ── TCP ─────────→ Collector buffer
   │ (2000ms) ── Flush ──────→ HDFS write
   │ (Unknown) ── HDFS reader ─→ Spark (no persistence)
   │ (Unknown) ── Spark ─────→ Console log
   └─ LOST (no database!)
   
Total Latency: 2.3+ seconds
Fate of Data: VOLATILE (only in logs)
```

### After Phase 1 (Week 1)
```
Bluesky Post
   │ (200ms) ── WebSocket ──→ Bridge
   │ (50ms) ─── TCP ────────→ Collector
   │ (50ms) ─── Kafka write ─→ Kafka
   │ (50ms) ─── Kafka read ──→ Spark
   │ (100ms) ─── NLP model ──→ Sentiment [POSITIVE/NEGATIVE/NEUTRAL]
   │ (50ms) ─── SQL write ───→ PostgreSQL
   │ (100ms) ─── Dashboard ──→ Real-time gauge updates
   └─ SUCCESS ✓ (persisted & visible)

Total Latency: <1 second (end-to-end)
Throughput: 100+ posts/second
Data Durability: PERSISTENT (PostgreSQL + HDFS backup)
```

### After Phase 2 (Week 2)
```
Same as Phase 1, PLUS:
   │
   ├─→ Parquet warehouse (for batch analytics)
   ├─→ Redis cache (for dashboard speedup)
   └─→ Kafka.enriched (for downstream consumers)

Query Examples:
  "Sentiment in last 24 hours?" → PostgreSQL (instant)
  "Trending topics?" → Redis cache (instant)
  "Historical analysis (3 months)?" → Parquet (fast)
```

### After Phase 3 (Week 3)
```
Same as Phase 2, PLUS:
   │
   ├─→ 5-minute windowed aggregations
   ├─→ Hourly trend detection
   ├─→ Anomaly alerts (sudden sentiment spikes)
   └─→ Emerging topics (TF-IDF based)
```

---

## 👥 Team Assignment (Assuming 3 Developer Team)

| Developer | Role | Primary Responsibilities |
|-----------|------|--------------------------|
| **Dev 1** | Backend/Kafka | Bridge, Collector, Kafka tuning, configuration |
| **Dev 2** | Data/Spark | Sentiment model, Spark pipelines, SQL queries |
| **Dev 3** | Infra/DevOps | Docker, PostgreSQL, monitoring, Kubernetes |

---

## ✅ Definition of Done (By Phase)

### Phase 1 Complete When:
- ✅ Sentiment labels appear in database
- ✅ Dashboard shows real-time gauge with sentiment distribution
- ✅ Sentiment accuracy ≥75% on test dataset
- ✅ E2E latency <2 seconds
- ✅ Throughput >100 posts/sec
- ✅ All tests passing
- ✅ Code reviewed and merged

### Phase 2 Complete When:
- ✅ All data in Parquet format
- ✅ PostgreSQL queries return <100ms
- ✅ Redis cache hit rate >80%
- ✅ Dashboard loads in <50ms
- ✅ Data retention policy enforced
- ✅ Backup/recovery tested

### Subsequent Phases:
Each phase has similar DoD checklist (see DEVELOPMENT_ROADMAP.md)

---

## 🚨 Critical Path (Minimum Viable Product)

To get to **"MVP ready for stakeholder demo"**:

```
Week 1: Phase 1 (Sentiment + DB + Dashboard)
        ├─ 2 days: Sentiment analysis
        ├─ 1 day: PostgreSQL setup
        ├─ 1.5 days: Streamlit dashboard
        └─ 0.5 days: Integration testing

DEMO READY! (Show: Real-time sentiment analysis, trending topics, dashboard)

Week 2: Phase 2 (Parquet + Windowing)
        ├─ 1 day: Format conversion
        ├─ 1.5 days: Time windows
        └─ 1.5 days: Aggregations UI

Week 3: Phase 4 (Monitoring - for stability)
        ├─ 1 day: Prometheus setup
        ├─ 1 day: Grafana dashboards
        └─ 1 day: Alerting

PRODUCTION READY! (Can support ongoing operations)
```

**Total Time to MVP: 1 week**  
**Total Time to Production: 3 weeks**

---

## 📚 Architecture Decision Records (ADRs)

### ADR-1: Kafka over Direct TCP ✅ (GOOD)
- **Decision:** Use Kafka as message broker instead of direct TCP
- **Reasoning:** Decoupling, fault tolerance, replayability
- **Status:** Implemented correctly

### ADR-2: HDFS + Database (Dual Storage) ⚠️ (GOOD BUT INCOMPLETE)
- **Decision:** Store raw data in HDFS, processed in DB
- **Reasoning:** Cost-effective for raw, responsive DB for queries
- **Status:** HDFS working, DB missing → Phase 2

### ADR-3: Spark Streaming (Stateful Processing) ⚠️ (INCOMPLETE)
- **Decision:** Use Spark for streaming + transformations
- **Reasoning:** Distributed, fault-tolerant, SQL support
- **Status:** Basic setup done, no sentiment/aggregations → Phase 1+3

### ADR-4: Docker Compose (Single Host) ⚠️ (DEVELOPMENT ONLY)
- **Decision:** Use docker-compose for local development
- **Reasoning:** Easy setup, good for prototyping
- **Status:** Perfect for dev, must migrate to K8s for prod → Phase 7

---

## 🎯 Key Metrics to Track

| Metric | Current | Target | Phase |
|--------|---------|--------|-------|
| **Sentiment Accuracy** | N/A | >75% | 1 |
| **E2E Latency** | Unknown | <2s | 1 |
| **Throughput** | Unknown | >1k posts/sec | 3 |
| **Data Durability** | Volatile | 99.9% | 2 |
| **Query Latency** | N/A | <100ms | 2 |
| **System Availability** | ~90% | >99.5% | 7 |
| **Monitoring Coverage** | 0% | >95% | 4 |
| **Test Coverage** | 0% | >70% | 6 |

---

## 📖 Reading Order

For maximum understanding, read in this order:

1. **This document** (Current state overview) ← START HERE
2. [DEVELOPMENT_ROADMAP.md](./DEVELOPMENT_ROADMAP.md) (8-phase plan)
3. [TECHNICAL_ANALYSIS.md](./TECHNICAL_ANALYSIS.md) (Deep dive into issues)
4. [IMPLEMENTATION_GUIDE.md](./IMPLEMENTATION_GUIDE.md) (How to code Phase 1)
5. Source code files (bridge.py, hdfs_collector.py, stream.py)

---

## 🚀 Start Here!

**For immediate action:**

1. Read [IMPLEMENTATION_GUIDE.md](./IMPLEMENTATION_GUIDE.md)
2. Implement Phase 1 (1 week maximum)
3. Have your first "real" demo with:
   - Live sentiment gauge
   - Trending topics
   - 24-hour chart
4. Proceed to Phase 2 (windowing + persistence)

**Questions?** Check [TECHNICAL_ANALYSIS.md](./TECHNICAL_ANALYSIS.md) or [DEVELOPMENT_ROADMAP.md](./DEVELOPMENT_ROADMAP.md)
