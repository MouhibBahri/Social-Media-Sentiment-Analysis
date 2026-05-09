"""batch_job.py

Batch processing job for Lambda batch layer:
- Topic discovery (embeddings + clustering)
- Historical trend & anomaly detection
- Event reconstruction

Reads raw JSONL posts from HDFS via WebHDFS, computes outputs, writes back to HDFS
and indexes results to Elasticsearch.
"""
import os
import time
import json
import math
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from io import BytesIO

import requests
from tqdm import tqdm
import numpy as np
from sklearn.cluster import MiniBatchKMeans
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import normalize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD

from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("batch")

# Config
HDFS_HOST = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT = int(os.getenv("HDFS_PORT", 9870))
HDFS_RAW_DIR = os.getenv("HDFS_RAW_DIR", "/bluesky/raw")
HDFS_OUT_DIR = os.getenv("HDFS_OUT_DIR", "/bluesky/processed")

ELASTIC_URL = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
ELASTIC_INDEX_TOPICS = os.getenv("ELASTIC_INDEX_TOPICS", "batch_topics")
ELASTIC_INDEX_TRENDS = os.getenv("ELASTIC_INDEX_TRENDS", "batch_trends")
ELASTIC_INDEX_EVENTS = os.getenv("ELASTIC_INDEX_EVENTS", "batch_events")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", 90))
DAY_BUCKETS = int(os.getenv("DAY_BUCKETS", 90))
CLUSTERING_K = int(os.getenv("CLUSTERING_K", 50))
EVENT_WINDOW_HOURS = int(os.getenv("EVENT_WINDOW_HOURS", 6))
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS", 300))

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")


def webhdfs_url(path: str, op: str, **params) -> str:
    base = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1{path}"
    qs = "&".join(f"{k}={v}" for k, v in {"op": op, **params}.items())
    return f"{base}?{qs}"


def list_hdfs_files(path: str):
    url = webhdfs_url(path, "LISTSTATUS")
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    files = []
    for e in data.get("FileStatuses", {}).get("FileStatus", []):
        if e.get("type") == "FILE":
            files.append(path.rstrip("/") + "/" + e.get("pathSuffix"))
        else:
            # directory — recurse
            subdir = path.rstrip("/") + "/" + e.get("pathSuffix")
            files.extend(list_hdfs_files(subdir))
    return files


def read_hdfs_file(path: str):
    url = webhdfs_url(path, "OPEN")
    r = requests.get(url, stream=True)
    r.raise_for_status()
    for line in r.iter_lines(decode_unicode=True):
        if line:
            yield json.loads(line)


def hdfs_mkdirs(path: str):
    url = webhdfs_url(path, "MKDIRS")
    r = requests.put(url)
    r.raise_for_status()


def hdfs_write(path: str, data: bytes):
    url = webhdfs_url(path, "CREATE", overwrite="true")
    r1 = requests.put(url, allow_redirects=False)
    if r1.status_code not in (307, 200, 201):
        raise RuntimeError(f"WebHDFS CREATE step1 failed: {r1.status_code} {r1.text}")
    location = r1.headers.get("Location", url)
    r2 = requests.put(location, data=BytesIO(data), headers={"Content-Type": "application/octet-stream"})
    r2.raise_for_status()


def ensure_hdfs_path(path: str):
    try:
        hdfs_mkdirs(path)
    except Exception:
        pass


def collect_posts_for_lookback(days=LOOKBACK_DAYS):
    """Collect posts from HDFS raw dir for the lookback window."""
    files = list_hdfs_files(HDFS_RAW_DIR)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    posts = []
    for f in files:
        # naive: attempt to include all files — user can prune via HDFS layout
        for post in read_hdfs_file(f):
            # expect posts to have `text` and `created_at` or `ts` fields
            created = post.get("created_at") or post.get("ts") or post.get("timestamp")
            try:
                # try parse ISO or numeric
                if isinstance(created, (int, float)):
                    dt = datetime.fromtimestamp(created / 1000.0, tz=timezone.utc)
                else:
                    dt = datetime.fromisoformat(created)
                if dt >= cutoff:
                    normalized = dict(post)
                    normalized["id"] = normalized.get("id") or normalized.get("uri") or hashlib.sha1(
                        f"{normalized.get('source', 'unknown')}|{dt.isoformat()}|{normalized.get('text', '')}".encode("utf-8")
                    ).hexdigest()
                    normalized["text"] = normalized.get("text", "")
                    normalized["created_at"] = dt.isoformat()
                    normalized["source"] = normalized.get("source", "unknown")
                    posts.append(normalized)
            except Exception:
                # if parse fails, include anyway
                normalized = dict(post)
                normalized["id"] = normalized.get("id") or normalized.get("uri") or hashlib.sha1(
                    f"{normalized.get('source', 'unknown')}|unknown|{normalized.get('text', '')}".encode("utf-8")
                ).hexdigest()
                normalized["text"] = normalized.get("text", "")
                normalized["created_at"] = None
                normalized["source"] = normalized.get("source", "unknown")
                posts.append(normalized)
    return posts


def preprocess_texts(texts):
    # simple cleanup
    cleaned = []
    for t in texts:
        if not t:
            cleaned.append("")
            continue
        s = t.lower()
        # remove urls and non-alphanum
        s = s.replace("\n", " ")
        s = " ".join(word for word in s.split() if not word.startswith("http"))
        cleaned.append(s)
    return cleaned


def compute_embeddings(texts, model_name=MODEL_NAME):
    # Prefer sentence-transformers if available (better semantic embeddings).
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer(model_name)
        emb = model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
        return emb
    except Exception:
        # Fallback: TF-IDF + TruncatedSVD to produce dense embeddings without heavy deps
        log.info("SentenceTransformer not available — using TF-IDF + SVD fallback for embeddings")
        tf = TfidfVectorizer(max_df=0.9, min_df=2, stop_words="english", ngram_range=(1,2), max_features=5000)
        X = tf.fit_transform(texts)
        svd = TruncatedSVD(n_components=min(128, X.shape[1]-1 or 1), random_state=42)
        emb = svd.fit_transform(X)
        emb = normalize(emb)
        return emb


def cluster_embeddings(embeddings, k=CLUSTERING_K):
    if len(embeddings) == 0:
        return np.array([]), None
    k = min(k, max(1, len(embeddings)//5))
    km = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=256)
    labels = km.fit_predict(embeddings)
    return labels, km.cluster_centers_



def extract_top_keywords(texts, top_n=10):
    if not texts:
        return [], []

    try:
        vec = CountVectorizer(max_df=0.5, min_df=2, stop_words="english", ngram_range=(1,2))
        X = vec.fit_transform(texts)
        names = np.array(vec.get_feature_names_out())
        counts = X.sum(axis=0).A1
        top_idx = counts.argsort()[::-1][:top_n]
        return list(names[top_idx]), counts[top_idx].tolist()
    except Exception:
        fallback = CountVectorizer(max_df=1.0, min_df=1, stop_words="english", ngram_range=(1,1))
        X = fallback.fit_transform(texts)
        names = np.array(fallback.get_feature_names_out())
        counts = X.sum(axis=0).A1
        top_idx = counts.argsort()[::-1][:top_n]
        return list(names[top_idx]), counts[top_idx].tolist()


def compute_topics(posts, embeddings, labels):
    # posts: list of dicts with id,text,created_at
    df_by_cluster = defaultdict(list)
    for p, lbl in zip(posts, labels):
        df_by_cluster[int(lbl)].append(p)

    topics = []
    total_posts = max(1, len(posts))
    for cid, items in df_by_cluster.items():
        texts = [p["text"] for p in items]
        keywords, counts = extract_top_keywords(texts, top_n=10)
        sources = Counter((p.get("source") or "unknown").strip().lower() for p in items)
        topics.append({
            "cluster_id": int(cid),
            "count": len(items),
            "keywords": keywords,
            "keywords_text": ", ".join(keywords[:5]),
            "topic_keywords_text": ", ".join(keywords[:10]),
            "topic_name": keywords[0] if keywords else f"topic-{cid}",
            "topic_share": round(len(items) / total_posts, 6),
            "platforms": sorted([src for src in sources.keys() if src]),
            "platform_counts": dict(sources),
            "top_source": sources.most_common(1)[0][0] if sources else "unknown",
            "sample_post_ids": [p.get("id") for p in items[:5]],
            "run_at": datetime.now(timezone.utc).isoformat(),
        })
    return topics


def compute_trends(posts):
    # build daily counts for keywords (top global keywords)
    texts = [p["text"] for p in posts]
    vec = CountVectorizer(max_df=0.9, min_df=5, stop_words="english", ngram_range=(1,1), max_features=5000)
    X = vec.fit_transform(texts)
    feature_names = vec.get_feature_names_out()

    # map post -> day index
    day_counts = defaultdict(Counter)
    for i, p in enumerate(posts):
        created = p.get("created_at")
        if created:
            try:
                dt = datetime.fromisoformat(created)
                day = dt.date().isoformat()
            except Exception:
                day = "unknown"
        else:
            day = "unknown"
        row = X.getrow(i)
        nz = row.nonzero()[1]
        for idx in nz:
            day_counts[day][feature_names[idx]] += row[0, idx]

    # compute baseline stats across days
    keyword_days = defaultdict(list)
    for day, counter in day_counts.items():
        for kw, cnt in counter.items():
            keyword_days[kw].append((day, cnt))

    trends = []
    run_at = datetime.now(timezone.utc).isoformat()
    for kw, day_list in keyword_days.items():
        day_list = sorted(day_list, key=lambda item: item[0])
        counts = np.array([c for d, c in day_list])
        latest_day, latest_count = day_list[-1]
        baseline_counts = counts[:-1] if len(counts) > 1 else counts
        baseline_mean = float(baseline_counts.mean())
        baseline_std = float(baseline_counts.std())
        deviation = float(latest_count - baseline_mean)
        zscore = float(deviation / baseline_std) if baseline_std > 0 else 0.0
        is_anomaly = bool(latest_count > baseline_mean + 3 * baseline_std and latest_count > baseline_mean)
        bursts = [d for d, c in day_list if c > baseline_mean + 3 * baseline_std]
        trends.append({
            "keyword": kw,
            "current_day": latest_day,
            "current_count": int(latest_count),
            "baseline_mean": baseline_mean,
            "baseline_std": baseline_std,
            "deviation": deviation,
            "deviation_pct": float(deviation / baseline_mean) if baseline_mean > 0 else 0.0,
            "zscore": zscore,
            "score": abs(zscore) if baseline_std > 0 else abs(deviation),
            "is_anomaly": is_anomaly,
            "bursts": bursts,
            "history": [{"day": d, "count": int(c)} for d, c in day_list],
            "run_at": run_at,
        })

    trends.sort(key=lambda item: item.get("score", 0), reverse=True)
    for rank, item in enumerate(trends, start=1):
        item["leaderboard_rank"] = rank

    return trends


def reconstruct_events(posts, embeddings, labels):
    # For each cluster, group temporally-close posts into events
    events = []
    posts_by_cluster = defaultdict(list)
    for p, lbl, emb in zip(posts, labels, embeddings):
        posts_by_cluster[int(lbl)].append((p, emb))

    for cid, items in posts_by_cluster.items():
        # sort by created_at
        items_sorted = []
        for p, emb in items:
            created = p.get("created_at")
            try:
                dt = datetime.fromisoformat(created)
            except Exception:
                dt = datetime.now(timezone.utc)
            items_sorted.append((dt, p, emb))
        items_sorted.sort(key=lambda x: x[0])

        # sliding window merge
        cur_event = None
        for dt, p, emb in items_sorted:
            if cur_event is None:
                cur_event = {"start": dt, "end": dt, "posts": [p], "embs": [emb]}
            else:
                if (dt - cur_event["end"]).total_seconds() <= EVENT_WINDOW_HOURS * 3600:
                    cur_event["end"] = dt
                    cur_event["posts"].append(p)
                    cur_event["embs"].append(emb)
                else:
                    keywords = extract_top_keywords([pp.get("text", "") for pp in cur_event["posts"]], top_n=10)[0]
                    sources = Counter((pp.get("source") or "unknown").strip().lower() for pp in cur_event["posts"])
                    peak_bucket = Counter(
                        dt_item.replace(second=0, microsecond=0).isoformat()
                        for dt_item, _, _ in items_sorted
                        if cur_event["start"] <= dt_item <= cur_event["end"]
                    ).most_common(1)
                    peak_timestamp = peak_bucket[0][0] if peak_bucket else cur_event["start"].isoformat()
                    duration_minutes = max(0.0, (cur_event["end"] - cur_event["start"]).total_seconds() / 60.0)
                    # finalize
                    intensity = math.sqrt(len(cur_event["posts"]))
                    events.append({
                        "cluster_id": cid,
                        "start": cur_event["start"].isoformat(),
                        "end": cur_event["end"].isoformat(),
                        "duration_minutes": duration_minutes,
                        "count": len(cur_event["posts"]),
                        "intensity": intensity,
                        "keywords": keywords,
                        "keywords_text": ", ".join(keywords[:5]),
                        "event_name": keywords[0] if keywords else f"event-{cid}",
                        "platforms": sorted([src for src in sources.keys() if src]),
                        "platforms_text": ", ".join(sorted([src for src in sources.keys() if src])) or "unknown",
                        "platform_count": len([src for src in sources.keys() if src]),
                        "platform_counts": dict(sources),
                        "peak_timestamp": peak_timestamp,
                        "sample_ids": [pp.get("id") for pp in cur_event["posts"][:5]],
                        "run_at": datetime.now(timezone.utc).isoformat(),
                    })
                    cur_event = {"start": dt, "end": dt, "posts": [p], "embs": [emb]}
        if cur_event:
            keywords = extract_top_keywords([pp.get("text", "") for pp in cur_event["posts"]], top_n=10)[0]
            sources = Counter((pp.get("source") or "unknown").strip().lower() for pp in cur_event["posts"])
            peak_bucket = Counter(
                dt_item.replace(second=0, microsecond=0).isoformat()
                for dt_item, _, _ in items_sorted
                if cur_event["start"] <= dt_item <= cur_event["end"]
            ).most_common(1)
            peak_timestamp = peak_bucket[0][0] if peak_bucket else cur_event["start"].isoformat()
            duration_minutes = max(0.0, (cur_event["end"] - cur_event["start"]).total_seconds() / 60.0)
            intensity = math.sqrt(len(cur_event["posts"]))
            events.append({
                "cluster_id": cid,
                "start": cur_event["start"].isoformat(),
                "end": cur_event["end"].isoformat(),
                "duration_minutes": duration_minutes,
                "count": len(cur_event["posts"]),
                "intensity": intensity,
                "keywords": keywords,
                "keywords_text": ", ".join(keywords[:5]),
                "event_name": keywords[0] if keywords else f"event-{cid}",
                "platforms": sorted([src for src in sources.keys() if src]),
                "platforms_text": ", ".join(sorted([src for src in sources.keys() if src])) or "unknown",
                "platform_count": len([src for src in sources.keys() if src]),
                "platform_counts": dict(sources),
                "peak_timestamp": peak_timestamp,
                "sample_ids": [pp.get("id") for pp in cur_event["posts"][:5]],
                "run_at": datetime.now(timezone.utc).isoformat(),
            })

    return events


def index_to_elasticsearch(es: Elasticsearch, index: str, docs: list):
    if not es.indices.exists(index=index):
        es.indices.create(index=index)
    actions = []
    for d in docs:
        actions.append({"_index": index, "_source": d})
    if actions:
        helpers.bulk(es, actions)


def write_jsonl_to_hdfs(path_prefix, objects):
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    hdfs_path = f"{HDFS_OUT_DIR}/{path_prefix}/{date_str}/{hour_str}"
    ensure_hdfs_path(hdfs_path)
    file_path = f"{hdfs_path}/batch_{int(now.timestamp()*1000)}.jsonl"
    payload = "\n".join(json.dumps(o) for o in objects) + "\n"
    hdfs_write(file_path, payload.encode("utf-8"))
    log.info("Wrote %d records to %s", len(objects), file_path)


def run_batch_once(es: Elasticsearch):
    log.info("Batch job starting — collecting posts from HDFS")
    posts = collect_posts_for_lookback(days=LOOKBACK_DAYS)
    if not posts:
        log.info("No posts found for lookback window — skipping this run")
        return

    texts = [p["text"] for p in posts]
    cleaned = preprocess_texts(texts)

    log.info("Computing embeddings for %d posts", len(cleaned))
    embeddings = compute_embeddings(cleaned)

    log.info("Clustering embeddings")
    labels, centers = cluster_embeddings(embeddings, k=CLUSTERING_K)

    log.info("Computing topics metadata")
    topics = compute_topics(posts, embeddings, labels)
    write_jsonl_to_hdfs("topics", topics)

    log.info("Computing trends and anomalies")
    trends = compute_trends(posts)
    write_jsonl_to_hdfs("trends", trends)

    log.info("Reconstructing events")
    events = reconstruct_events(posts, embeddings, labels)
    write_jsonl_to_hdfs("events", events)

    try:
        index_to_elasticsearch(es, ELASTIC_INDEX_TOPICS, topics)
        index_to_elasticsearch(es, ELASTIC_INDEX_TRENDS, trends)
        index_to_elasticsearch(es, ELASTIC_INDEX_EVENTS, events)
        log.info("Indexed batch outputs into Elasticsearch")
    except Exception as e:
        log.exception("Failed to index to Elasticsearch: %s", e)


def main():
    es = Elasticsearch([ELASTIC_URL])

    try:
        while True:
            run_batch_once(es)
            if BATCH_INTERVAL_SECONDS <= 0:
                break
            log.info("Sleeping %d seconds until the next batch run", BATCH_INTERVAL_SECONDS)
            time.sleep(BATCH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log.info("Interrupted — stopping batch loop")
    finally:
        log.info("Batch job stopped")


if __name__ == "__main__":
    main()
