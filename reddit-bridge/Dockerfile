FROM python:3.11-slim

WORKDIR /app
COPY reddit_bridge.py .

RUN pip install --no-cache-dir aiohttp kafka-python

CMD ["python", "reddit_bridge.py"]
