FROM python:3.11-slim

WORKDIR /app

COPY hdfs_collector.py .

RUN pip install --no-cache-dir requests kafka-python

CMD ["python", "hdfs_collector.py"]