"""
hdfs_collector.py
─────────────────
Connects to Kafka and writes incoming posts
into HDFS, partitioned by date and hour:

  /bluesky/raw/YYYY-MM-DD/HH/posts_<timestamp>.jsonl

Uses WebHDFS REST API.
"""

import os
import socket
import json
import time
import logging
from datetime import datetime, timezone
from io import BytesIO
from kafka import KafkaConsumer
import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ── Config (overridable via env vars) ────────────────────────────────────────
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", 9092))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bluesky.posts")
# How many posts to buffer before flushing a file to HDFS
FLUSH_EVERY  = int(os.getenv("FLUSH_EVERY", 50))

HDFS_HOST    = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT    = int(os.getenv("HDFS_PORT", 9870))
HDFS_DIR     = os.getenv("HDFS_DIR", "/bluesky/raw")
METRICS_PORT = int(os.getenv("METRICS_PORT", 8000))

# ── Prometheus metrics ────────────────────────────────────────────────────────
POSTS_CONSUMED = Counter(
    "collector_posts_consumed_total",
    "Posts consumed from Kafka and buffered",
)
HDFS_FLUSHES = Counter(
    "collector_hdfs_flushes_total",
    "Number of successful HDFS flushes",
)
HDFS_FLUSH_ERRORS = Counter(
    "collector_hdfs_flush_errors_total",
    "Number of failed HDFS flushes",
)
HDFS_FLUSH_DURATION = Histogram(
    "collector_hdfs_flush_duration_seconds",
    "Latency of HDFS flush operations",
)
BUFFER_SIZE = Gauge(
    "collector_buffer_size",
    "Number of posts currently buffered before next flush",
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── WebHDFS helpers ───────────────────────────────────────────────────────────

def webhdfs_url(path: str, op: str, **params) -> str:
    base = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1{path}"
    qs   = "&".join(f"{k}={v}" for k, v in {"op": op, **params}.items())
    return f"{base}?{qs}"


def hdfs_mkdirs(path: str):
    """Create a directory (and parents) in HDFS."""
    url = webhdfs_url(path, "MKDIRS")
    r   = requests.put(url)
    r.raise_for_status()


def hdfs_write(path: str, data: bytes):
    """
    Write bytes to a new file in HDFS via WebHDFS two-step redirect.
    If the file already exists it will be overwritten.
    """
    url = webhdfs_url(path, "CREATE", overwrite="true")
    # Step 1 — get the redirect location (datanode URL)
    r1  = requests.put(url, allow_redirects=False)
    if r1.status_code not in (307, 200, 201):
        raise RuntimeError(f"WebHDFS CREATE step1 failed: {r1.status_code} {r1.text}")

    location = r1.headers.get("Location", url)

    # Step 2 — upload the actual data
    r2 = requests.put(
        location,
        data=BytesIO(data),
        headers={"Content-Type": "application/octet-stream"},
    )
    r2.raise_for_status()


def flush_to_hdfs(buffer: list):
    """Serialize the buffer as JSONL and write to a date/hour partitioned path."""
    now       = datetime.now(timezone.utc)
    date_str  = now.strftime("%Y-%m-%d")
    hour_str  = now.strftime("%H")
    ts        = int(now.timestamp() * 1000)

    hdfs_path = f"{HDFS_DIR}/{date_str}/{hour_str}"
    file_path = f"{hdfs_path}/posts_{ts}.jsonl"

    with HDFS_FLUSH_DURATION.time():
        try:
            # Ensure directory exists
            hdfs_mkdirs(hdfs_path)

            payload = "\n".join(json.dumps(post) for post in buffer) + "\n"
            hdfs_write(file_path, payload.encode("utf-8"))

            HDFS_FLUSHES.inc()
            log.info("Flushed %d posts → %s", len(buffer), file_path)
        except Exception:
            HDFS_FLUSH_ERRORS.inc()
            raise


# ── Main loop ─────────────────────────────────────────────────────────────────
def connect_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="hdfs-collector-group",
        fetch_max_bytes=200000000,
        max_partition_fetch_bytes=200000000,
    )
    return consumer

def main():
    log.info("HDFS Collector starting — consume from kafka %s:%d topic=%s", KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC)
    start_http_server(METRICS_PORT)
    log.info("Prometheus metrics exposed on :%d/metrics", METRICS_PORT)
    consumer = connect_consumer()
    buffer   = []

    try:
        for msg in consumer:
            post = msg.value
            buffer.append(post)
            POSTS_CONSUMED.inc()
            BUFFER_SIZE.set(len(buffer))

            if len(buffer) >= FLUSH_EVERY:
                flush_to_hdfs(buffer)
                buffer.clear()
                BUFFER_SIZE.set(0)

    except KeyboardInterrupt:
        log.info("Interrupted — flushing remaining buffer…")
    finally:
        if buffer:
            flush_to_hdfs(buffer)
        consumer.close()
        log.info("Collector stopped.")


if __name__ == "__main__":
    main()
