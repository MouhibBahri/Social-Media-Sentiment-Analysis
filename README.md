# Social Sentiment Analysis — Big Data Pipeline
 
A real-time and historical sentiment analysis platform built on a Lambda Architecture. The system streams live posts from Bluesky, classifies their sentiment using a pretrained NLP model, and serves both live and historical dashboards.
 
---
 
## Overview
 
This project ingests a continuous stream of Bluesky posts, runs sentiment classification (positive / negative / neutral) on each post, and stores the results for two types of analysis:
 
- **Real-time**: what is the public feeling *right now*, updated every few seconds
- **Historical**: how has sentiment evolved over days and weeks, computed nightly
---
 
## Architecture

![Architecture Diagram](./images/architecture.png)



## Bluesky Bridge (`bluesky_api.py`)

A lightweight TCP bridge that connects the [Bluesky Jetstream](https://docs.bsky.app/docs/advanced-guides/firehose) WebSocket to a local socket server, making the stream consumable by Spark (or any TCP client).

It filters incoming public posts by keyword and forwards matching ones as JSON lines on port `9999`.

### Test it

Start the bridge:
```bash
python bluesky_api.py
```

Then in a second terminal, connect with netcat to see live posts stream in:
```bash
nc localhost 9999
```
## Batch Layer (Hadoop HDFS)

### 1. Start bridge on your machine
```bash
python bluesky_bridge.py
```

### 2. Start the Hadoop cluster + collector
```bash
docker compose up -d
```

### 3. Check the NameNode Web UI
```bash
open http://localhost:9870
```

### 4. Browse what's been saved in HDFS
```bash
docker exec namenode hdfs dfs -ls -R /bluesky/raw
```
