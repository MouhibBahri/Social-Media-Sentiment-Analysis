import asyncio
import websockets
import json
import os
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Gauge, start_http_server

BLUESKY_URI = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
KEYWORDS = ["microsoft", "copilot", "linux", "ai"]
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bluesky.posts")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# ── Prometheus metrics ────────────────────────────────────────────────────────
EVENTS_RECEIVED = Counter(
    "bridge_events_received_total",
    "Raw events received from upstream feed",
    ["source"],
)
MESSAGES_PRODUCED = Counter(
    "bridge_messages_produced_total",
    "Messages successfully produced to Kafka",
    ["source", "topic"],
)
MESSAGES_FILTERED_OUT = Counter(
    "bridge_messages_filtered_out_total",
    "Messages dropped because they did not match keyword filters",
    ["source"],
)
PRODUCER_ERRORS = Counter(
    "bridge_producer_errors_total",
    "Errors encountered while producing to Kafka",
    ["source"],
)
UPSTREAM_CONNECTED = Gauge(
    "bridge_upstream_connected",
    "1 if connected to upstream feed, 0 otherwise",
    ["source"],
)


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                max_request_size=200000000,
                request_timeout_ms=30000,
                linger_ms=100,
                api_version=(2, 6, 0),
            )
            print("Connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            print("Kafka broker unavailable, retrying in 5s...")
            time.sleep(5)


producer = create_producer()

async def stream_to_kafka():
    while True:   # reconnect loop
        try:
            async with websockets.connect(BLUESKY_URI, open_timeout=30) as ws:
                print("Connected to Bluesky.")
                UPSTREAM_CONNECTED.labels(source="bluesky").set(1)
                while True:
                    raw = await ws.recv()
                    event = json.loads(raw)
                    if event.get("kind") == "commit" and \
                       event["commit"].get("operation") == "create":
                        EVENTS_RECEIVED.labels(source="bluesky").inc()
                        text = event["commit"].get("record", {}).get("text", "")
                        if any(kw in text.lower() for kw in KEYWORDS):
                            payload = {
                                "text": text,
                                "author": event.get("did", "unknown"),
                                "created_at": datetime.utcnow().isoformat(),
                                "source": "bluesky"
                            }
                            try:
                                producer.send(KAFKA_TOPIC, payload)
                                MESSAGES_PRODUCED.labels(source="bluesky", topic=KAFKA_TOPIC).inc()
                            except Exception as send_err:
                                PRODUCER_ERRORS.labels(source="bluesky").inc()
                                print(f"Kafka send failed: {send_err}")
                        else:
                            MESSAGES_FILTERED_OUT.labels(source="bluesky").inc()
        except Exception as e:
            UPSTREAM_CONNECTED.labels(source="bluesky").set(0)
            print(f"Bluesky connection lost: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)   # wait then retry

def main():
    print("Bridge Kafka producer starting…")
    start_http_server(METRICS_PORT)
    print(f"Prometheus metrics exposed on :{METRICS_PORT}/metrics")
    asyncio.run(stream_to_kafka())

main()
