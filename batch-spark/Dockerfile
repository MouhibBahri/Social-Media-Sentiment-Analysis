FROM apache/spark:3.5.0-python3

WORKDIR /app

COPY requirements.txt ./

# Run installs as root (image default user is non-root), then switch back to spark user
USER root
ENV PIP_CACHE_DIR=/tmp/.pip-cache
RUN python3 -m pip install --upgrade pip setuptools wheel && \
    apt-get update && apt-get install -y build-essential wget git libsndfile1 && \
    pip3 install --no-cache-dir -r requirements.txt && \
    apt-get remove -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/* /tmp/.pip-cache

COPY batch_job.py ./

RUN chown -R spark:spark /app /home/spark || true
USER spark

ENV PYTHONUNBUFFERED=1
CMD ["python3", "batch_job.py"]
