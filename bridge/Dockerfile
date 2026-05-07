FROM python:3.11-slim

WORKDIR /app
COPY bridge.py .

RUN pip install --no-cache-dir websockets kafka-python prometheus-client

EXPOSE 8000

CMD ["python", "-u", "bridge.py"]