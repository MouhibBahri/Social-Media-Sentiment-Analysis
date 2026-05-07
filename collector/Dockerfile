FROM python:3.11-slim

WORKDIR /app

COPY hdfs_collector.py .

RUN pip install --no-cache-dir requests kafka-python prometheus-client

EXPOSE 8000

CMD ["python", "-u", "hdfs_collector.py"]