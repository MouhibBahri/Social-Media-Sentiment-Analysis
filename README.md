# 🌊 Real-Time Bluesky Streaming Data Pipeline (HDFS + Spark)

A distributed real-time data engineering pipeline that ingests live posts from Bluesky, streams them through a custom TCP bridge, stores them in HDFS, and processes them using Apache Spark Structured Streaming.

---

## 🚀 Overview

This project demonstrates a full end-to-end real-time data pipeline:

Bluesky API  
→ WebSocket Bridge (Python)  
→ TCP Socket Stream  
→ HDFS Collector (Python)  
→ HDFS (Data Lake storage)  
→ Apache Spark Structured Streaming  
→ Real-time analytics output

---

## 🧱 Architecture

Bluesky Stream  
→ Bridge (WebSocket → TCP)  
→ TCP :9999  
→ Collector (buffer + write)  
→ HDFS (partitioned storage)  
→ Spark Master + Worker  
→ Spark Streaming Job  

---

## 📦 Components

### 🔵 Bluesky Bridge
- Connects to Bluesky Jetstream WebSocket
- Filters post events
- Streams JSON over TCP

### 🟡 HDFS Collector
- Connects to bridge via TCP
- Buffers messages
- Writes JSONL files to HDFS
- Partitions by date/hour:
  /bluesky/raw/YYYY-MM-DD/HH/

### 🟢 Hadoop Cluster
- NameNode + 2 DataNodes
- Stores raw data
- Web UI available on port 9870

### 🟣 Apache Spark Streaming
- Reads new files from HDFS
- Structured Streaming
- Performs real-time aggregation
- Outputs results to console

---

## ⚙️ Tech Stack

- Python 3.11
- Docker & Docker Compose
- Apache Hadoop (HDFS)
- Apache Spark 3.5 (official image)
- WebSockets
- TCP Sockets
- WebHDFS API

---

## 📁 Project Structure
```
Social-Media-Sentiment-Analysis
├── bridge/
│   ├── Dockerfile
│   └── bridge.py
├── collector/
│   ├── Dockerfile
│   └── hdfs_collector.py
├── spark/
│   ├── Dockerfile
│   └── stream.py
├── hadoop-config/
│   ├── core-site.xml
│   └── hdfs-site.xml
├── docker-compose.yml
├── hadoop.env
└── README.md
```
---

## 🚀 How to Run

### 1. Clone repo
```bash
git clone <repo-url>
cd project
```

### 2. Start system
```bash
docker compose up --build
```

---

## 🌐 Access UIs

- HDFS NameNode: http://localhost:9870  
- Spark UI: http://localhost:8080  

---

## 📊 Expected Output

### HDFS files

/bluesky/raw/YYYY-MM-DD/HH/posts_*.jsonl

Example content:

```json
{"text": "hello world"}
{"text": "iphone event live"}
```

---

### Spark output

-------------------------------------------
Batch: 0
-------------------------------------------
|text                |count|
| ------------------ | --- |
|apple event         |12   |
|iphone leak         |7    |


---

## 🔄 Data Flow

1. Bluesky streams posts
2. Bridge forwards via TCP
3. Collector buffers messages
4. Writes JSONL to HDFS
5. Spark reads new files
6. Real-time aggregation

---

## 🧠 Key Features

- Real-time ingestion pipeline
- Fault-tolerant buffering
- Distributed storage (HDFS)
- Streaming analytics (Spark)
- Time-partitioned data
- Fully containerized system

---

## 🧪 Testing

### Check bridge
```bash
docker logs -f bluesky-bridge
```

### Check collector
```bash
docker logs -f hdfs-collector
```

### Check HDFS
http://localhost:9870

### Check Spark
```bash
docker logs -f spark-app
```

---

## 🛠️ Common Issues

### Bridge not reachable
- Ensure BRIDGE_HOST=bluesky-bridge
- Port 9999 exposed

### Spark crash
- Missing schema in stream.py
- Wrong HDFS config

### No Spark output
- Spark only reads new files after startup

---

## 📈 Future Improvements

- Kafka replacement for TCP layer
- Sentiment analysis (NLP)
- Real-time dashboard (Streamlit)
- Parquet instead of JSONL
- Windowed streaming analytics

---

## 👨‍💻 Author

Big Data project covering:
- Streaming ingestion
- Distributed storage
- Real-time processing
- Container orchestration

---

## 📜 License

MIT