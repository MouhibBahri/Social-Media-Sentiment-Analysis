# 🔍 Technical Codebase Analysis & Architecture Review

## 📦 Project Overview

**Project Name:** Social Sentiment Analysis — Big Data Streaming Pipeline  
**Architecture Type:** Lambda Architecture (Real-time + Batch)  
**Tech Stack:** Python 3.11, Kafka, Hadoop HDFS, Apache Spark, Docker Compose  
**Purpose:** Ingest Bluesky social media posts in real-time, perform sentiment analysis, and provide streaming analytics

---

## 🏗️ System Architecture Components

### 1. Bluesky Bridge (`bridge/bridge.py`)

**Purpose:** Connects to Bluesky Jetstream WebSocket and publishes posts to Kafka

**Current Implementation:**
```python
- WebSocket client to Bluesky Jetstream (wss://jetstream2.us-east.bsky.network)
- Filters: Only app.bsky.feed.post events
- Keyword filtering: ["apple", "iphone"] (hardcoded)
- Output: KafkaProducer → bluesky.posts topic
- Message format: {"text": "..."}
- Reconnection logic: 5-second retry on disconnect
```

**Issues & Improvements Needed:**
| Issue | Severity | Impact | Fix |
|-------|----------|--------|-----|
| Hardcoded keywords | Medium | Not configurable via env vars | Move to `KEYWORDS` env var |
| No error logging | High | Difficult to debug issues | Add proper logging with timestamps |
| Blocking I/O | Medium | Single async loop | Already async, but add error recovery |
| No metrics | Medium | Can't monitor throughput | Add counter metrics |
| No schema validation | Low | Unexpected field names cause issues | Add try/except on field access |

**Recommendations:**
- ✅ Make keywords configurable via environment variables
- ✅ Implement structured logging (JSON format)
- ✅ Add Prometheus metrics (posts_received_total, connection_failures)
- ✅ Add graceful shutdown handler

---

### 2. HDFS Collector (`collector/hdfs_collector.py`)

**Purpose:** Consumes Kafka messages and writes batched data to HDFS

**Current Implementation:**
```
✓ KafkaConsumer → bluesky.posts topic
✓ Buffer posts until FLUSH_EVERY (default: 50)
✓ WebHDFS REST API for writes
✓ Partition by date/hour: /bluesky/raw/YYYY-MM-DD/HH/posts_<timestamp>.jsonl
✓ Graceful shutdown with final flush
✓ Proper logging
```

**Issues & Improvements Needed:**
| Issue | Severity | Impact | Fix |
|-------|----------|--------|-----|
| JSONL format (not Parquet) | Medium | Slower queries, larger file sizes | Convert to Parquet |
| No data validation | High | Bad data reaches HDFS | Add schema validation |
| No retry logic for HDFS | High | Single write failure crashes component | Implement exponential backoff |
| Consumer never commits offset | Medium | Can reprocess old messages | Enable auto_commit=True |
| No metrics | Medium | Can't monitor output | Add written_posts_total metric |
| Hardcoded FLUSH_EVERY | Low | Can't tune without restart | Already env-configurable ✓ |

**Schema Issues:**
```python
# Current schema (implicit)
{"text": "..."}

# Should be:
{
  "id": "uuid",
  "text": "...",
  "author": "...",
  "created_at": "ISO8601",
  "source": "bluesky",
  "language": "en"
}
```

**Recommendations:**
- ✅ Upgrade output format: JSONL → Parquet
- ✅ Implement schema enforcement
- ✅ Add WebHDFS write retry logic
- ✅ Implement health checks
- ✅ Add metrics collection (Prometheus)

---

### 3. Spark Streaming (`spark/stream.py`)

**Purpose:** Real-time processing of Kafka streams with aggregations

**Current Implementation:**
```python
✓ Spark Structured Streaming from Kafka
✓ Schema: {text: String}
✓ Aggregation: groupBy(text).count()
✓ Output: Console (complete mode)
✓ Bootstrap servers: kafka:9092
```

**Critical Issues:**
| Issue | Severity | Impact | Fix |
|-------|----------|--------|-----|
| ⚠️ Console output only | Critical | No data persistence! | Add multiple sinks (DB, Parquet, Kafka) |
| No sentiment analysis | Critical | Missing core feature | Integrate NLP model |
| Incomplete mode loses old data | High | Trends disappear | Use update/append mode + windowing |
| No error handling | High | Job crashes silently | Add try/except, checkpointing |
| No schema inference | Medium | Type mismatches | Hard-code schema |
| No checkpointing | High | Can lose data on failure | Enable writeStream checkpoint |
| No watermarking | Medium | Late data handling unclear | Add watermark() for late arrivals |

**Current Output:**
```
Batch: 0
+-------------------+-----+
|text               |count|
+-------------------+-----+
|apple event        |12   |
|iphone leak        |7    |
+-------------------+-----+
```

**Problems:**
- `complete()` mode re-outputs everything each batch (inefficient)
- New insights lost on next batch (no state management)
- No timestamp tracking
- No sentiment classification

**Recommendations:**
- ✅ Add sentiment analysis (NLP model as UDF)
- ✅ Change output mode: complete → append/update with windows
- ✅ Add multiple output sinks (PostgreSQL, Parquet, Kafka)
- ✅ Implement checkpoint directory for fault tolerance
- ✅ Add watermarking for late-arriving data
- ✅ Optimize by removing duplicate text grouping (use hashing)

---

### 4. Kafka Configuration

**Current Setup:**
```yaml
- KRaft mode (Broker + Controller combined)
- Single broker (node_id=1)
- Topic: bluesky.posts (3 partitions, 1 replication factor)
- Bootstrap servers: kafka:9092
```

**Issues:**

| Issue | Severity | Context |
|-------|----------|---------|
| Single partition replication | Medium | No fault tolerance; broker failure = data loss |
| No authentication | Medium | No security (fine for dev, not prod) |
| No topic retention policy | Low | Kafka stores indefinitely (disk space) |
| Auto topic creation disabled | Low | Requires explicit topic creation |

**Recommendations:**
- ⚠️ Increase replication-factor to 2+ (in production)
- ✅ Set retention policy (e.g., 7 days for raw posts)
- ✅ Enable authentication (SASL in production)
- ✅ Monitor broker metrics

---

### 5. HDFS Configuration

**Current Setup:**
```
- NameNode (single, no HA)
- 2 DataNodes
- Replication factor: 2
- WebHDFS enabled: ✓
- Permissions disabled: ✓ (dev-only setting)
```

**Issues:**

| Issue | Severity | Context |
|-------|----------|---------|
| No HA (High Availability) | High | NameNode failure = entire cluster offline |
| Single NameNode metadata | High | No backup; metadata loss = total data loss |
| Permissions disabled | Medium | Security risk in production |
| No Kerberos auth | Low | Fine for internal data center |

**Recommendations:**
- ⚠️ Add HA setup (Secondary NameNode → HA QJM)
- ✅ Enable permissions in production
- ✅ Set replication-factor: 3 (for production)
- ✅ Monitor NameNode heap usage
- ⚠️ Implement regular backups of NameNode metadata

---

### 6. Network & Deployment

**Current Setup:**
```yaml
- Docker Compose (single host)
- Shared bridge network: hadoop-net
- Port exposed: 9999 (bridge), 9092 (kafka), 9870 (HDFS), 8081 (Spark)
```

**Issues:**

| Issue | Severity | Context |
|-------|----------|---------|
| Single host (no distributed deployment) | Medium | Can't scale across machines |
| No Kubernetes (manual docker commands) | Medium | Difficult operational management |
| All services depend on single Docker daemon | High | Single point of failure |
| No service discovery beyond Docker DNS | Low | Works fine within Docker network |

**Recommendations:**
- Phase 7: Migrate to Kubernetes
- ✅ Use Helm charts for package management
- ✅ Implement proper resource limits
- ✅ Add Persistent Volumes for data durability

---

## 🔐 Data Quality & Validation Issues

### Current State
```
Bluesky → (no validation) → Kafka → (no validation) → HDFS
```

**Missing Validations:**
1. **Schema enforcement** — No StructType validation in Spark
2. **Null checking** — `text` field could be null, empty, or extremely long
3. **Character encoding** — Can receive emoji, RTL text, special characters
4. **Duplicate detection** — Same post could be sent multiple times
5. **Freshness checks** — Old posts could be replayed from Kafka
6. **Completeness** — Missing author, timestamp, source metadata
7. **Anomaly detection** — Posts with unusual patterns (bot behavior)

**Recommendations:**
- ✅ Add schema validation in collector before HDFS write
- ✅ Handle null/empty strings gracefully
- ✅ Implement deduplication logic (using post ID)
- ✅ Add data quality metrics
- ✅ Implement automatic DLQ (dead-letter queue) for invalid records

---

## 📊 Observability & Monitoring

### Current State
```
❌ No metrics collection (Prometheus)
❌ No centralized logging (ELK/CloudWatch)
❌ No health check endpoints
❌ No alerting system
❌ Only docker logs available
```

**Critical Gaps:**
1. **Throughput visibility** — Can't measure posts/sec
2. **Latency visibility** — No E2E latency tracking
3. **Error tracking** — No error rate metrics
4. **Resource usage** — Can't monitor CPU/memory per service
5. **Data freshness** — No indication of lag

**Recommendations (Phase 4):**
- ✅ Add Prometheus + Grafana
- ✅ Instrument all components with metrics
- ✅ Implement distributed tracing (Jaeger)
- ✅ Centralize logs (ELK or CloudWatch)
- ✅ Set up alerting rules (PagerDuty integration)

---

## 🧪 Testing & Quality Assurance

### Current State
```
❌ No unit tests
❌ No integration tests
❌ No load testing
❌ No code coverage metrics
❌ No CI/CD pipeline
```

**Risks:**
- Changes can break existing functionality
- No regression detection
- Performance degradation undetected
- Deployment failures possible

**Recommendations (Phase 6):**
- ✅ Add pytest for unit tests (target: 70%+ coverage)
- ✅ Create integration test suite (docker-compose up, test, down)
- ✅ Implement load testing (simulate 1k+ posts/sec)
- ✅ Add CI/CD pipeline (GitHub Actions, GitLab CI)
- ✅ Code quality checks (Pylint, Black formatting)

---

## 🔧 Operational Issues & Resiliency

### Failure Scenarios & Recovery

| Failure Mode | Current Behavior | Impact | Recovery Time |
|--------------|------------------|--------|----------------|
| Bridge crashes | Reconnects after 5s | ≤5s data loss | 5-10 seconds |
| Kafka broker down | Collector blocks, buffering | 1-2 min data loss | Manual restart |
| HDFS unavailable | Write fails, process crashes | All data lost until restart | 1-5 minutes |
| Spark job fails | Console stops, no recovery | Streaming halts | Manual restart |
| Network partition | Services can't communicate | Complete pipeline halt | Network restoration |

**Issues:**
1. **No circuit breaker** — Services don't fail gracefully
2. **No dead-letter queue** — Failed messages are lost
3. **No automatic recovery** — Requires manual intervention
4. **No backup/fallback** — Single path, no redundancy

**Recommendations:**
- ✅ Implement circuit breaker pattern
- ✅ Add event retry with exponential backoff
- ✅ Create DLQ for failed messages
- ✅ Implement health probes + auto-restart
- ✅ Add backup data sources/failover

---

## 📈 Performance & Optimization Opportunities

### Current Performance Characteristics

| Metric | Current | Target | Gap |
|--------|---------|--------|-----|
| E2E Latency | Unknown | <2s | Needs measurement |
| Throughput | Unknown | 1k+ posts/sec | Needs measurement |
| Storage (JSONL) | 100% | 20% (Parquet) | 5:1 compression ratio |
| Query latency | No DB | <100ms | Needs implementation |
| Spark batch interval | Unknown | 1-5 sec | Needs tuning |

### Optimization Opportunities

1. **Parquet vs. JSONL**
   - Switch to Parquet: 5:1 compression, 10x faster queries
   - Estimate: 2-3 day effort, immediate storage savings

2. **Kafka partitioning**
   - Current: 3 partitions, no key
   - Improvement: Partition by keyword/sentiment (after analysis)
   - Estimate: 1 day effort, better parallelism

3. **Caching strategy**
   - Add Redis for trending results (TTL: 1-2 hours)
   - Cache warm results, avoid repeated computation
   - Estimate: 2-3 day effort, 10x query speedup

4. **Spark tuning**
   - Batch interval: currently unknown, recommend 5-10 seconds
   - Partitions: auto-tune based on parallelism
   - Executor memory: need to benchmark
   - Estimate: 1-2 day effort, potentially 2-3x throughput

---

## 🎯 Risk Assessment

### High-Risk Issues

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| **Data Loss** (HDFS failure) | Medium | Critical | Add HA NameNode, implement backups |
| **Single Point of Failure** (Docker host) | Medium | Critical | Deploy to Kubernetes, multi-zone |
| **Memory Leak** (Spark streaming) | Medium | High | Implement monitoring, set memory limits |
| **Kafka topic full** (no retention) | Low | High | Set retention policy to 7 days |
| **Sentiment model outdated** | High (N/A yet) | Medium | Implement model versioning, A/B testing |

### Medium-Risk Issues

- No authentication (security)
- No end-to-end encryption (data in transit)
- Hard-coded configuration values
- No feature flags for deployments

---

## 🚀 Architecture Strengths

✅ **Good decisions:**
- Event-driven architecture (Kafka)
- Distributed storage (HDFS)
- Streaming framework (Spark)
- Containerized (Docker Compose)
- Time-based partitioning (efficient queries)
- Fault-tolerant buffering (collector)
- Async WebSocket client (bridge)

---

## 📋 Summary of Issues by Category

### Code Quality (Medium Priority)
- [ ] Lacking unit tests and integration tests
- [ ] No structured logging
- [ ] Hardcoded configuration defaults
- [ ] Minimal error handling and exception recovery
- [ ] No code documentation or docstrings

### Data Quality (High Priority)
- [ ] No schema validation
- [ ] No duplicate detection
- [ ] Missing metadata (author, timestamp, language)
- [ ] No data freshness guarantees
- [ ] Format not optimized (JSONL vs. Parquet)

### Operational Gaps (High Priority)
- [ ] No monitoring or metrics collection
- [ ] No alerting system
- [ ] No health checks
- [ ] No automatic recovery mechanisms
- [ ] No centralized logging

### Feature Gaps (Critical Priority)
- [ ] **No sentiment analysis** (core feature!)
- [ ] No output persistence
- [ ] No dashboard or visualization
- [ ] No API for historical queries
- [ ] No alerting on anomalies

### Infrastructure Gaps (High Priority)
- [ ] No HA setup (single NameNode, single Kafka broker)
- [ ] Not production-ready (no Kubernetes)
- [ ] No backup/disaster recovery
- [ ] Single Docker host (SPOF)

---

## 🎬 Immediate Action Items (Next Week)

1. **Sentiment Analysis (Phase 1.1)** — Core feature
   - Effort: 2-3 days
   - Impact: High (enables all downstream features)

2. **Schema and Validation (Phase 1.1)** — Data quality
   - Effort: 1-2 days
   - Impact: Medium (prevents silent failures)

3. **Multiple Output Sinks (Phase 2)** — Data persistence
   - Effort: 3-4 days
   - Impact: High (enables analytics)

4. **Structured Logging (Phase 4.1)** — Observability
   - Effort: 1-2 days
   - Impact: Medium (debugging, monitoring)

---

## 📚 Technology Debt

**Current Technical Debt: Moderate**

| Component | Debt Level | Cost of Fixing |
|-----------|-----------|----------------|
| No sentiment model | High | 3 days |
| JSONL format | High | 2 days (data migration) |
| No persistence | Critical | 2 days |
| No monitoring | High | 2-3 days |
| No tests | Medium | 1-2 weeks |
| Docker-only deployment | Medium | 1 week (Kubernetes) |

**Estimated total tech debt:** 3-4 weeks of development

---

## 👥 Team Recommendations

**Optimal team composition for executing roadmap:**
- 1 **Backend engineer** (Python, Kafka, HDFS)
- 1 **Data engineer** (Spark, SQL, data pipelines)
- 1 **DevOps engineer** (Docker, Kubernetes, monitoring)
- 1 **Frontend engineer** (optional, for dashboard)

**Timeline:** 8-12 weeks with full team
