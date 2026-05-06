import asyncio
import json
import os
import time
from datetime import datetime

import aiohttp
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

REDDIT_URL = os.getenv("REDDIT_URL", "https://www.reddit.com/r/all/new.json?limit=100")
KEYWORDS = ["microsoft", "copilot", "linux", "ai"]
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "reddit.posts")
POLL_INTERVAL = int(os.getenv("REDDIT_POLL_SECONDS", "15"))


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
    seen_ids = set()

    async with aiohttp.ClientSession(headers={"User-Agent": "reddit-kafka-bridge/0.1"}) as session:
        while True:
            try:
                async with session.get(REDDIT_URL, timeout=30) as response:
                    data = await response.json()
                    posts = data.get("data", {}).get("children", [])
                    for item in posts:
                        post = item.get("data", {})
                        post_id = post.get("id")
                        if not post_id or post_id in seen_ids:
                            continue

                        title = post.get("title", "")
                        selftext = post.get("selftext", "")
                        text = f"{title}\n{selftext}".strip()
                        created_utc = post.get("created_utc")
                        created_at = None
                        if created_utc:
                            try:
                                created_at = datetime.utcfromtimestamp(float(created_utc)).isoformat()
                            except Exception:
                                created_at = datetime.utcnow().isoformat()
                        else:
                            created_at = datetime.utcnow().isoformat()

                        if any(keyword in text.lower() for keyword in KEYWORDS):
                            payload = {
                                "id": post_id,
                                "text": text,
                                "author": post.get("author", "unknown"),
                                "created_at": created_at,
                                "source": "reddit",
                                "subreddit": post.get("subreddit", "unknown"),
                                "permalink": post.get("permalink", ""),
                            }
                            producer.send(KAFKA_TOPIC, payload)
                            seen_ids.add(post_id)

                    if len(seen_ids) > 5000:
                        seen_ids.clear()

            except Exception as exc:
                print(f"Reddit bridge error: {exc}. Retrying in 5s...")
                await asyncio.sleep(5)
                continue

            await asyncio.sleep(POLL_INTERVAL)


def main():
    print("Reddit bridge Kafka producer starting…")
    asyncio.run(stream_to_kafka())


if __name__ == "__main__":
    main()
