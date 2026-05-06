# 📦 Dependencies & Requirements Matrix

## Current Dependencies Status

### ✅ Already Installed (Working)
- ✓ Python 3.11
- ✓ `websockets` (bridge WebSocket client)
- ✓ `kafka-python` (Kafka producer/consumer)
- ✓ `requests` (WebHDFS API calls)
- ✓ `apache/spark:3.5.0` (official Docker image)
- ✓ `apache/kafka:3.9.1` (Kafka broker)
- ✓ `bde2020/hadoop` (HDFS cluster)
- ✓ Docker & Docker Compose

### ❌ Missing (Required for Phases 1+)

| Phase | Package | Purpose | Python/System | Status |
|-------|---------|---------|---|---|
| **1** | `transformers` | Sentiment classification (NLP) | Python | CRITICAL |
| **1** | `torch` | Deep learning backend for transformers | Python | CRITICAL |
| **1** | `psycopg2` | PostgreSQL driver | Python | CRITICAL |
| **1** | `sqlalchemy` | ORM for DB operations | Python | HIGH |
| **2** | `pandas` | Data manipulation | Python | HIGH |
| **2** | `redis` | Redis client library | Python | MEDIUM |
| **3** | `scikit-learn` | ML utilities (anomaly detection, TF-IDF) | Python | MEDIUM |
| **4** | `prometheus_client` | Metrics export | Python | MEDIUM |
| **5** | `streamlit` | Dashboard framework | Python | HIGH |
| **5** | `plotly` | Interactive charts for Streamlit | Python | HIGH |
| **8** | `mlflow` | ML model versioning | Python | MEDIUM |
| - | PostgreSQL 15 | Database (Docker image) | System | CRITICAL |
| - | Redis 7.x | Cache layer (Docker image) | System | MEDIUM |
| - | Prometheus | Metrics collection (Docker image) | System | MEDIUM |
| - | Grafana | Visualization (Docker image) | System | HIGH |

---

## Phase 1 Setup (Next Week)

### Python Packages to Add

Create `requirements.txt`:
```
# Existing
websockets>=10.0
kafka-python>=2.0.2
requests>=2.31.0

# Phase 1: Sentiment Analysis
transformers>=4.30.0  # HuggingFace models
torch>=2.0.0  # or tensorflow
torchaudio  # for transformers

# Phase 1: Database
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0

# Phase 2: Data processing
pandas>=2.0.0
redis>=4.5.0

# Phase 3: Analytics
scikit-learn>=1.3.0
numpy>=1.24.0
scipy>=1.10.0

# Phase 4: Monitoring
prometheus-client>=0.17.0

# Phase 5: Dashboard
streamlit>=1.28.0
plotly>=5.17.0

# General
python-dotenv>=1.0.0  # .env file support
logging-json>=0.5  # JSON logging
```

### Docker Compose Changes

Add to `docker-compose.yml`:

```yaml
  # Phase 1: PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    container_name: postgres
    environment:
      POSTGRES_DB: bluesky_analytics
      POSTGRES_USER: analytics
      POSTGRES_PASSWORD: password123
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/init.sql  # Schema
    networks:
      - hadoop-net
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U analytics"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  # Phase 2: Redis Cache
  redis:
    image: redis:7-alpine
    container_name: redis
    networks:
      - hadoop-net
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  # Phase 4: Prometheus Metrics
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
    networks:
      - hadoop-net
    ports:
      - "9090:9090"
    restart: unless-stopped

  # Phase 4: Grafana Visualization
  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/datasources:/etc/grafana/provisioning/datasources
    networks:
      - hadoop-net
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
    restart: unless-stopped

  # Phase 5: Streamlit Dashboard
  dashboard:
    build: ./dashboard
    container_name: streamlit-dashboard
    networks:
      - hadoop-net
    ports:
      - "8501:8501"
    depends_on:
      - postgres
    environment:
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: bluesky_analytics
      DB_USER: analytics
      DB_PASSWORD: password123
    restart: unless-stopped

# Add volumes
volumes:
  postgres_data:
  redis_data:
  prometheus_data:
  grafana_data:
  # existing volumes...
```

### Updated Dockerfiles

**bridge/Dockerfile** (Phase 1):
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY bridge.py .
COPY requirements-bridge.txt .

RUN pip install --no-cache-dir -r requirements-bridge.txt

CMD ["python", "bridge.py"]
```

`requirements-bridge.txt`:
```
websockets>=10.0
kafka-python>=2.0.2
python-dotenv>=1.0.0
```

**collector/Dockerfile** (Phase 1):
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY hdfs_collector.py .
COPY requirements-collector.txt .

RUN pip install --no-cache-dir -r requirements-collector.txt

CMD ["python", "hdfs_collector.py"]
```

`requirements-collector.txt`:
```
kafka-python>=2.0.2
requests>=2.31.0
psycopg2-binary>=2.9.0
python-dotenv>=1.0.0
prometheus-client>=0.17.0
```

**spark/Dockerfile** (Phase 1+):
```dockerfile
FROM apache/spark:3.5.0-python3

WORKDIR /app
COPY stream.py .
COPY requirements-spark.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-spark.txt

# Add JDBC driver for PostgreSQL
RUN mkdir -p /opt/spark/jars && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -O /opt/spark/jars/postgresql.jar

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--conf", "spark.jars.ivy=/tmp/ivy", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--jars", "/opt/spark/jars/postgresql.jar", \
     "/app/stream.py"]
```

`requirements-spark.txt`:
```
transformers>=4.30.0
torch>=2.0.0
pandas>=2.0.0
scikit-learn>=1.3.0
redis>=4.5.0
python-dotenv>=1.0.0
prometheus-client>=0.17.0
```

**dashboard/Dockerfile** (Phase 5):
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY app.py .
COPY requirements-dashboard.txt .

RUN pip install --no-cache-dir -r requirements-dashboard.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

`requirements-dashboard.txt`:
```
streamlit>=1.28.0
plotly>=5.17.0
pandas>=2.0.0
psycopg2-binary>=2.9.0
redis>=4.5.0
python-dotenv>=1.0.0
```

---

## Database Schema (init-db.sql)

```sql
-- Create tables for Phase 1+

CREATE TABLE IF NOT EXISTS posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    text TEXT NOT NULL,
    author VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'bluesky',
    language VARCHAR(10) DEFAULT 'en',
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sentiment_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    post_id UUID REFERENCES posts(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    sentiment VARCHAR(20) NOT NULL, -- POSITIVE, NEGATIVE, NEUTRAL
    sentiment_score FLOAT NOT NULL, -- 0.0-1.0
    confidence FLOAT NOT NULL,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_version VARCHAR(50),
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS aggregations_hourly (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hour TIMESTAMP NOT NULL,
    sentiment VARCHAR(20) NOT NULL,
    count INTEGER NOT NULL,
    avg_score FLOAT,
    min_score FLOAT,
    max_score FLOAT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trending_topics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    keyword VARCHAR(255) NOT NULL,
    frequency INTEGER NOT NULL,
    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,
    trend_score FLOAT,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for performance
CREATE INDEX idx_sentiment_results_created_at ON sentiment_results(computed_at);
CREATE INDEX idx_sentiment_results_sentiment ON sentiment_results(sentiment);
CREATE INDEX idx_aggregations_hourly_hour ON aggregations_hourly(hour);
CREATE INDEX idx_trending_topics_keyword ON trending_topics(keyword);
CREATE INDEX idx_posts_created_at ON posts(created_at);
```

---

## Configuration Files to Create

### prometheus.yml
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'bridge'
    static_configs:
      - targets: ['bluesky-bridge:8000']

  - job_name: 'collector'
    static_configs:
      - targets: ['hdfs-collector:8001']

  - job_name: 'spark'
    static_configs:
      - targets: ['spark-app:4040']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9999']
```

### grafana/datasources/prometheus.yml
```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true
```

### grafana/dashboards/bluesky.json
```json
(See IMPLEMENTATION_GUIDE.md for Grafana dashboard JSON)
```

### Environment Variables (hadoop.env)

Update to include:
```env
# Existing
CORE_CONF_fs_defaultFS=hdfs://namenode:8020
...

# Phase 1+: Database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=bluesky_analytics
POSTGRES_USER=analytics
POSTGRES_PASSWORD=password123

# Phase 2: Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_TTL=3600

# Phase 4: Prometheus
PROMETHEUS_HOST=prometheus
PROMETHEUS_PORT=9090

# Spark
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
```

---

## Installation Checklist

### Before Phase 1 Starts:

- [ ] Create `requirements.txt` with all packages
- [ ] Create `requirements-*.txt` for each service (bridge, collector, spark, dashboard)
- [ ] Update `docker-compose.yml` with PostgreSQL, Redis, Prometheus, Grafana
- [ ] Create `init-db.sql` with schema
- [ ] Create `prometheus.yml` config
- [ ] Create `hadoop.env` with new variables
- [ ] Test build: `docker compose build --no-cache`
- [ ] Test up: `docker compose up -d`
- [ ] Verify PostgreSQL health: `docker exec postgres pg_isready -U analytics`
- [ ] Verify Prometheus: `http://localhost:9090`
- [ ] Verify Grafana: `http://localhost:3000` (admin/admin)

### During Phase 1:

- [ ] Install sentiment model on first Spark startup (auto-download)
- [ ] Create PostgreSQL tables (via init-db.sql)
- [ ] Test Spark writes to PostgreSQL
- [ ] Test dashboard data queries

### Model Download Considerations

The first time Spark runs with transformers, it will download ~400MB model:
```python
# Expected: 2-5 minutes on first run
# Subsequent: <1 second (cached in Docker layer)

from transformers import pipeline
classifier = pipeline("sentiment-analysis", 
                     model="distilbert-base-uncased-finetuned-sst-2-english")
# Downloads to ~/.cache/huggingface/hub/
```

**Optimize:** Pre-download in Dockerfile:
```dockerfile
RUN python -c "from transformers import pipeline; \
    pipeline('sentiment-analysis', \
    model='distilbert-base-uncased-finetuned-sst-2-english')"
```

---

## Dependency Tree (What Depends on What)

```
Python Environment
├── websockets ──→ Bridge (WebSocket client)
├── kafka-python → Bridge, Collector, Spark (Kafka I/O)
├── requests ────→ Collector (WebHDFS)
├── psycopg2 ────→ Collector, Dashboard (PostgreSQL)
├── sqlalchemy ──→ Collector (ORM)
├── transformers → Spark (Sentiment analysis)
├── torch ───────→ transformers (Deep learning)
├── pandas ──────→ Spark, Dashboard (Data manipulation)
├── scikit-learn → Spark (Anomaly detection)
├── redis ───────→ Collector, Spark (Cache)
├── prometheus_client → All (Metrics)
├── streamlit ───→ Dashboard (UI)
└── plotly ──────→ Dashboard (Charts)

Docker Services
├── Kafka ───────→ Bridge, Collector, Spark consume
├── PostgreSQL ──→ Collector writes, Dashboard reads
├── Redis ───────→ Spark caches, Dashboard queries cache
├── Prometheus ──→ Scrapes metrics from all services
├── Grafana ─────→ Queries Prometheus for visualization
└── HDFS ────────→ Collector writes, Spark can read
```

---

## Storage Requirements Estimation

| Component | Phase 1 | Phase 2 | Phase 3+ |
|-----------|---------|---------|----------|
| HDFS (raw posts) | 10GB/day | 10GB/day | 10GB/day |
| PostgreSQL | 2GB | 5GB | 20GB |
| Redis (cache) | 500MB | 500MB | 500MB |
| Parquet warehouse | - | 5GB/day | 5GB/day |
| Prometheus metrics | - | 100MB | 500MB |
| Grafana dashboards | - | 100MB | 100MB |
| **TOTAL** | **12.6GB** | **20.6GB** | **35.6GB** |

---

## Version Compatibility Matrix

| Service | Version | Reason | Min | Max |
|---------|---------|--------|-----|-----|
| Python | 3.11 | Async support, modern features | 3.9 | 3.12 |
| Kafka | 3.9.1 | Latest stable, KRaft | 3.6 | 4.0 |
| Spark | 3.5.0 | Native Kafka support | 3.4 | Latest |
| PostgreSQL | 15 | Modern, stable | 13 | 16 |
| Redis | 7 | Streams support | 6.2 | latest |
| transformers | 4.30+ | Pre-trained models | 4.20 | latest |
| torch | 2.0+ | Efficient, modern | 1.13 | latest |
| Kafka-Python | 2.0.2 | Stability | 2.0 | 2.1 |

---

## Troubleshooting: Missing Dependencies

### If Spark fails to start:
```
ERROR: ModuleNotFoundError: No module named 'transformers'

Solution:
1. Update spark/Dockerfile with transformers in RUN pip
2. Rebuild: docker compose build spark-app
3. Restart: docker compose up spark-app
```

### If PostgreSQL connection fails:
```
ERROR: psycopg2.OperationalError: could not connect to server

Solution:
1. Ensure postgres container is running: docker compose ps
2. Ensure healthcheck passes: docker logs postgres
3. Check credentials in hadoop.env match docker-compose.yml
4. Wait 30s for postgres to initialize
```

### If Prometheus scrape fails:
```
ERROR: 404 Not Found from target

Solution:
1. services must expose /metrics endpoint
2. Add prometheus_client library to all services
3. Add metric export code to each service __main__
```

---

## Network Ports Reference

| Port | Service | Purpose | Status |
|------|---------|---------|--------|
| 9999 | Bridge | TCP socket (debug only) | Current |
| 9092 | Kafka | Bootstrap servers | Current |
| 9870 | HDFS | NameNode WebUI + WebHDFS | Current |
| 7077 | Spark | Master RPC | Current |
| 8081 | Spark | Master WebUI | Current |
| 5432 | PostgreSQL | Database | Phase 1+ |
| 6379 | Redis | Cache | Phase 2+ |
| 9090 | Prometheus | Metrics scraper | Phase 4+ |
| 3000 | Grafana | Visualization | Phase 4+ |
| 8501 | Streamlit | Dashboard | Phase 5+ |

---

## Next Steps

1. **Copy this file** to your project root
2. **Create all missing files** (requirements.txt, init-db.sql, etc.)
3. **Run: `docker compose build --no-cache`**
4. **Run: `docker compose up -d`**
5. **Verify all services healthy**
6. **Proceed with Phase 1 implementation** (see IMPLEMENTATION_GUIDE.md)

Start coding Phase 1 sentiment analysis! 🚀
