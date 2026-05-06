import asyncio
import websockets
import json
import os
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BLUESKY_URI = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
KEYWORDS = ["Microsoft", "Copilot", "Linux", "microsoft", "AI"]
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bluesky.posts")


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
                while True:
                    raw = await ws.recv()
                    event = json.loads(raw)
                    if event.get("kind") == "commit" and \
                       event["commit"].get("operation") == "create":
                        text = event["commit"].get("record", {}).get("text", "")
                        if any(kw in text.lower() for kw in KEYWORDS):
                            payload = {
                                "text": text,
                                "author": event["commit"].get("record", {}).get("author", "unknown"),
                                "created_at": datetime.utcnow().isoformat(),
                                "source": "bluesky"
                            }
                            producer.send(KAFKA_TOPIC, payload)
        except Exception as e:
            print(f"Bluesky connection lost: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)   # wait then retry

def main():
    print("Bridge Kafka producer starting…")
    asyncio.run(stream_to_kafka())

main()
