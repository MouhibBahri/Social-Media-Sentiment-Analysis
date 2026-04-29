from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("BlueskyStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("text", StringType(), True)
])

kafka_bootstrap = "kafka:9092"
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "bluesky.posts") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

query = df_json.groupBy("text").count() \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()