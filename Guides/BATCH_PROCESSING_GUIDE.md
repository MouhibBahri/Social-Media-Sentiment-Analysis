# Batch Processing Guide

## Overview

This guide explains the batch analytics layer for the Social Media Sentiment Analysis project.

The batch job in `batch-spark/batch_job.py` is a valid Lambda-style batch layer that complements the real-time Spark streaming pipeline.
It reads raw JSONL posts from HDFS, performs higher-level analytics, writes output back to HDFS, and indexes results to Elasticsearch for batch visualization.

---

## Why this belongs in the project

The project is designed as a hybrid streaming + batch analytics solution.
The real-time layer handles immediate sentiment scoring and persistence in PostgreSQL.
The batch layer provides historical, topic-level, anomaly, and event reconstruction analysis over past data.

This is important because:
- Streaming is good for real-time insight and alerting.
- Batch is good for historical trend analysis, clustering, and anomaly detection.
- Both layers together form a robust analytics architecture.

---

## What the batch job does

### Input
- Reads raw post data from HDFS via WebHDFS.
- Source path: `HDFS_RAW_DIR` (default `/bluesky/raw`).
- Expects each line to be a JSON object with at least `text`, `created_at`, or timestamp fields.

### Processing
- Normalizes posts to a canonical shape with `id`, `text`, `created_at`, and `source`.
- Cleans text and removes URLs.
- Computes embeddings using:
  - `sentence-transformers` if available, otherwise
  - `TF-IDF + TruncatedSVD` fallback.
- Clusters embeddings into topic groups with `MiniBatchKMeans`.
- Extracts top keywords and platform/source metadata for each topic.
- Computes trend/anomaly statistics for keywords over time.
- Reconstructs temporal events per cluster using a sliding time window.

### Output
- Writes JSONL results to HDFS under `HDFS_OUT_DIR` (default `/bluesky/processed`).
- Indexes results into Elasticsearch indices:
  - `batch_topics`
  - `batch_trends`
  - `batch_events`

---

## Integration readiness

### Storage sink

The batch job is ready for a storage sink because:
- It already writes processed output back to HDFS.
- It indexes results to Elasticsearch via the configured `ELASTIC_URL`.
- The Docker Compose setup already includes the `elasticsearch` service.

### Visualization layer

The batch job is also ready to be visualized because:
- Results are stored in searchable Elasticsearch indices.
- A visualization layer can query those indices directly.
- Grafana is provisioned with batch dashboards for:
  - exploding trend anomalies
  - topic share market segments
  - reconstructed event timelines

### Grafana setup

Grafana is provisioned to use Elasticsearch as a datasource for batch analytics.
The dashboards under `monitoring/grafana/dashboards` are ready to visualize the `batch_*` indices.

1. Start the stack.
2. Open Grafana at `http://localhost:3000`.
3. Log in with `admin` / `admin`.
4. Confirm the Elasticsearch datasources are available and point to `http://elasticsearch:9200`.
5. Use the pre-provisioned dashboards such as `Batch Analytics (Topics / Trends / Events)`.

If needed, a custom dashboard can still be built on top of Elasticsearch using Streamlit or another UI, but Grafana is now the preferred visualization layer for batch analytics.

---

## Deployment

### Docker Compose

The batch service is defined in `docker-compose.yml` as `batch-spark`.
It depends on:
- `hdfs-init` (HDFS ready)
- `elasticsearch` (service healthy)

Environment variables can be tuned in `docker-compose.yml`:
- `HDFS_RAW_DIR`
- `HDFS_OUT_DIR`
- `LOOKBACK_DAYS`
- `BATCH_INTERVAL_SECONDS`
- `CLUSTERING_K`
- `EVENT_WINDOW_HOURS`

### Batch container

The batch container image is built from `batch-spark/Dockerfile`.
Required Python dependencies are listed in `batch-spark/requirements.txt`.

Currently installed dependencies include:
- `requests`
- `scikit-learn`
- `numpy`
- `scipy`
- `elasticsearch`
- `tqdm`

If `sentence-transformers` is desired for higher-quality embeddings, add it to `requirements.txt`.

---

## Practical next steps

1. Run the batch job after HDFS has raw posts.
2. Verify Elasticsearch indices are populated:
   - `batch_topics`
   - `batch_trends`
   - `batch_events`
4. Use the pre-provisioned Grafana dashboards to inspect batch analytics.
5. Optionally extend the Grafana dashboards with custom panels for your use case.

---

## Recommended batch visualization targets

### Topics dashboard
- Top clusters by post count
- Top keywords per cluster
- Platform/source distribution per cluster

### Trends dashboard
- Keyword trend counts over time
- Anomaly scores and z-scores
- Burst days and surprising keyword activity

### Events dashboard
- Reconstructed event timelines
- Event intensity and duration
- Sample post IDs per event
- Source/platform breakdown per event

---

## Notes / caveats

- If no raw HDFS files are present, the batch job will skip processing.
- The job assumes raw input is JSONL and may require schema validation if the source changes.
- Elasticsearch should be reachable at the configured `ELASTIC_URL`.
- This batch job is not a replacement for the streaming sentiment path; it is a complementary analytics layer.
