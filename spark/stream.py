from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("BlueskyStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("text", StringType(), True)
])

df = spark.readStream \
    .schema(schema) \
    .json("hdfs://namenode:8020/bluesky/raw/*/*")

query = df.groupBy("text").count() \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()