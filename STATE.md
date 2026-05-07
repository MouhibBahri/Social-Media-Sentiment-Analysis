# Social Media Sentiment Analysis - Project State

## 📊 PROJECT STATUS OVERVIEW

**Current Phase:** Phase 1 - Sentiment Analysis Foundation + Observability
**Overall Progress:** 75% → 90%
**Timeline:** Week 1 of 8-week roadmap
**Status:** 🟢 ACTIVE — Kafka + Spark + PostgreSQL + Streamlit + Prometheus + Grafana

---

## 🎯 PHASE 1: SENTIMENT ANALYSIS FOUNDATION (90% Complete)

### ✅ COMPLETED

| Feature | Status | Document Reference | Notes |
|---------|--------|-------------------|-------|
| **Bridge Services** | ✅ DONE | `bridge/bridge.py`, `reddit-bridge/reddit_bridge.py` | Bluesky + Reddit Kafka producers |
| **Kafka Topic Setup** | ✅ DONE | `docker-compose.yml` | `bluesky.posts` + `reddit.posts` |
| **Spark Streaming Pipeline** | ✅ DONE | `spark/stream.py` | Multi-topic ingest, source-aware processing |
| **Data Cleaning & Validation** | ✅ DONE | `spark/stream.py` | Trim, whitespace normalize, schema validate |
| **PostgreSQL Persistence** | ✅ DONE | `postgres-init.sql` | `sentiment_posts` + `sentiment_aggregate` |
| **Streamlit Dashboard (live data)** | ✅ DONE | `dashboard/app.py` | Time-window selector, system status, auto-refresh, switched filters to `processed_at` |
| **Prometheus stack** | ✅ DONE | `docker-compose.yml`, `monitoring/prometheus/prometheus.yml` | Scrapes bridges, collector, spark, kafka-exporter, postgres-exporter, cAdvisor |
| **Grafana provisioning** | ✅ DONE | `monitoring/grafana/**` | Auto-provisioned datasources + 2 dashboards |
| **App-level Prometheus metrics** | ✅ DONE | `bridge/bridge.py`, `reddit-bridge/reddit_bridge.py`, `collector/hdfs_collector.py`, `spark/stream.py` | Counters, gauges, histograms exposed on `:8000/metrics` |
| **Docker Compose Orchestration** | ✅ DONE | `docker-compose.yml` | All services wired on `hadoop-net` |

### 🔄 IN PROGRESS

| Feature | Status | Document Reference | Progress | ETA |
|---------|--------|-------------------|----------|-----|
| **Kafka + Spark throughput tuning** | 🔄 | `spark/stream.py` | 60% | 2 hours |

### ⏳ PENDING (Phase 1)

| Feature | Status | Document Reference | Priority |
|---------|--------|-------------------|----------|
| **Unit tests / validation scripts** | ⏳ | `tests/` | HIGH |
| **Historical / batch analytics job** | ⏳ | `spark/` (new batch job) | MEDIUM |
| **Spark checkpoint resiliency (HDFS)** | ⏳ | `spark/stream.py` | MEDIUM |
| **Alertmanager rules** | ⏳ | `monitoring/prometheus/` | LOW |

---

## 🚧 BLOCKERS & ISSUES

### Resolved Issues
- ✅ Fixed Kafka payload mapping for Reddit and Bluesky
- ✅ Added `source` to persisted rows and aggregates
- ✅ Source-aware PostgreSQL schema
- ✅ Streamlit dashboard rendered empty because it filtered on `created_at` with a 1-hour window, but Reddit `created_at` is the post's original timestamp (often older than 1h) — switched to `processed_at` and added a configurable time window plus all-time totals
- ✅ Streamlit had no auto-refresh — added 15s rerun loop and manual refresh
- ✅ No observability — added Prometheus + Grafana stack with app-level metrics
- ✅ Validated `docker compose config` syntax after monitoring additions

### Active Issues
- (none currently)

---

## 📈 PROGRESS METRICS

### Code Quality
- **Test Coverage:** 0% (Unit tests pending)
- **Documentation:** 95%
- **Code Standards:** 90%

### Performance Targets
- **Sentiment Analysis:** real-time streaming ingest (foreachBatch, ~seconds)
- **Data Processing:** Kafka → Spark → PostgreSQL
- **Storage:** HDFS raw archive + PostgreSQL analytics
- **Observability:** Prometheus 15s scrape, Grafana auto-provisioned dashboards

### Success Criteria Met
- ✅ Multiple bridge producers working
- ✅ Spark consumes `bluesky.posts` and `reddit.posts`
- ✅ Source-aware persistence in `sentiment_posts`
- ✅ Dashboard now displays live data
- ✅ Pipeline metrics visible in Grafana

---

## 📅 DEVELOPMENT TIMELINE

**May 5, 2026:**
- ✅ Base Spark sentiment pipeline
- ✅ PostgreSQL persistence
- ✅ Initial dashboard

**May 6, 2026:**
- ✅ Reddit bridge + Kafka topic
- ✅ Source-aware analytics and persistence
- ✅ Spark data cleaning + validation

**May 7, 2026:**
- ✅ Added Prometheus + Grafana monitoring stack
- ✅ Instrumented bridges, collector and Spark with `prometheus_client` metrics
- ✅ Provisioned 2 Grafana dashboards (Pipeline Health + Sentiment Analytics)
- ✅ Fixed empty Streamlit dashboard (time window selector, `processed_at` filters, system status panel, auto-refresh)

**Next:**
- Add a separate batch Spark job over HDFS for historical analytics
- Add unit and integration tests
- Add Prometheus alert rules

---

## 🔧 TECHNICAL IMPLEMENTATION STATUS

### Core Components
- **Bluesky Bridge:** ✅ Working, exposes `/metrics` on `:8000`
- **Reddit Bridge:** ✅ Working, exposes `/metrics` on `:8000`
- **HDFS Collector:** ✅ Working, exposes `/metrics` on `:8000`
- **Kafka (KRaft):** ✅ With topic initialization
- **Spark Streaming:** ✅ Multi-topic ingest + enrichment, driver exposes `/metrics`
- **PostgreSQL:** ✅ Source-aware analytics schema
- **Streamlit Dashboard:** ✅ Live data, time window selector, auto-refresh
- **Prometheus:** ✅ Scraping all components every 15s
- **Grafana:** ✅ Auto-provisioned datasources + dashboards
- **postgres-exporter / kafka-exporter / cAdvisor:** ✅ Wired to Prometheus

### Dependencies
- **NLTK VADER:** ✅
- **Spark 3.5.0:** ✅
- **Kafka 3.9.1:** ✅ — 2 topics
- **HDFS:** ✅ — raw storage
- **PostgreSQL 15:** ✅
- **Prometheus 2.54.1, Grafana 11.2.0:** ✅

### Data Flow
```
Bluesky/Reddit → Bridges → Kafka → Spark Streaming → PostgreSQL → Streamlit
                              ↓
                       HDFS Collector → HDFS (raw archive)

All services → /metrics → Prometheus → Grafana
```

---

## 📝 NOTES & DECISIONS

### Architecture Decisions
- **Multi-source ingestion** (Bluesky + Reddit)
- **Source-aware analytics**
- **Real-time path only** for now (`foreachBatch` writes to Postgres). HDFS is a raw archive for a future batch layer.
- **Observability:** Prometheus pull model, Grafana provisioned at boot, two ready-made dashboards
- **Streamlit time filtering:** uses `processed_at` (always set by Spark on ingest) instead of `created_at` so the UI does not show empty for old upstream timestamps

### Implementation Notes
- `spark/stream.py` reads topics from `KAFKA_TOPICS`
- All Python services start an HTTP server on `:8000` exposing Prometheus metrics
- Grafana datasource UIDs are `Prometheus` and `SentimentPostgres` and are referenced by the provisioned dashboards
- Auto-refresh in Streamlit is implemented as a 15s `time.sleep` + `st.rerun()` at the end of the script

---

*Last Updated: May 7, 2026*
*Next Update: After adding batch analytics layer / tests*
