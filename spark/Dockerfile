FROM apache/spark:3.5.0

WORKDIR /app
COPY stream.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--conf", "spark.jars.ivy=/tmp/ivy", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--deploy-mode", "client", \
     "/app/stream.py"]