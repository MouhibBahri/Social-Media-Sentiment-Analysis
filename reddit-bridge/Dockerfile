FROM python:3.11-slim

WORKDIR /app
COPY reddit_bridge.py .

RUN pip install --no-cache-dir aiohttp kafka-python prometheus-client

EXPOSE 8000

CMD ["python", "-u", "reddit_bridge.py"]
