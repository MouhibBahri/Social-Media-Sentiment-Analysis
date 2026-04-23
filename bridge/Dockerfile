FROM python:3.11-slim

WORKDIR /app
COPY bridge.py .

RUN pip install --no-cache-dir websockets

CMD ["python", "bridge.py"]