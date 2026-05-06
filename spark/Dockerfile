FROM apache/spark:3.5.0-python3

# 1. Switch to root to perform installations
USER root

WORKDIR /app

# 2. Copy files while owned by root
COPY stream.py .
COPY requirements-spark.txt .

# 3. Install Python dependencies
RUN pip install --no-cache-dir -r requirements-spark.txt

# 4. Download NLTK data into a shared location accessible by the spark user
ENV NLTK_DATA=/usr/local/share/nltk_data
RUN mkdir -p ${NLTK_DATA} && \
    python3 -m nltk.downloader -d ${NLTK_DATA} vader_lexicon

# 5. Add JDBC driver
RUN mkdir -p /opt/spark/jars && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -O /opt/spark/jars/postgresql.jar

# 6. Optional: Switch back to the spark user for security
# Ensure the app directory is accessible by the spark user
RUN chown -R spark:spark /app
USER spark

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--conf", "spark.jars.ivy=/tmp/ivy", \
     "--conf", "spark.driver.memory=512m", \
     "--conf", "spark.executor.memory=512m", \
     "--conf", "spark.executor.memoryOverhead=128m", \
     "--conf", "spark.executor.cores=1", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--jars", "/opt/spark/jars/postgresql.jar", \
     "/app/stream.py"]