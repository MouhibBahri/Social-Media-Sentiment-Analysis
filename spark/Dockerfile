FROM apache/spark:3.5.0

WORKDIR /app
COPY stream.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--deploy-mode", "client", \
     "/app/stream.py"]