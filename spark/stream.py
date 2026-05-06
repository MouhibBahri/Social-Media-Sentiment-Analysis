import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, udf, current_timestamp, when, length, trim, to_date
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize NLTK VADER sentiment analyzer
try:
    sia = SentimentIntensityAnalyzer()
    logger.info("NLTK VADER sentiment analyzer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize VADER: {e}")
    raise

# PostgreSQL Connection Configuration
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_USER = "bluesky"
POSTGRES_PASSWORD = "bluesky_postgres_pwd"
POSTGRES_DB = "sentiment_analytics"

def get_postgres_connection():
    """Establish PostgreSQL connection with retry logic."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB
        )
        logger.info("PostgreSQL connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def write_sentiment_aggregates_to_postgres(batch_df, batch_id):
    """Write sentiment aggregations to PostgreSQL."""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        
        # Collect aggregation data
        agg_data = batch_df.collect()
        
        # Prepare data for insertion
        current_time = datetime.utcnow()
        values = []
        
        for row in agg_data:
            sentiment = row['sentiment']
            source = row['source'] if row['source'] else 'unknown'
            count = row['count']
            values.append((current_time, source, sentiment, count))
        
        if values:
            # Insert or update sentiment aggregates per source
            sql = """
            INSERT INTO sentiment_aggregate (timestamp, source, sentiment, count)
            VALUES %s
            ON CONFLICT (timestamp, source, sentiment)
            DO UPDATE SET count = EXCLUDED.count
            """
            execute_values(cur, sql, values)
            conn.commit()
            logger.info(f"Batch {batch_id}: Wrote {len(values)} sentiment aggregates to PostgreSQL")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error writing aggregates to PostgreSQL: {e}")
        # Don't raise - allow streaming to continue even if DB write fails

def write_sentiment_posts_to_postgres(batch_df, batch_id):
    """Write individual posts with sentiment scores to PostgreSQL."""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        
        # Collect post data
        posts_data = batch_df.collect()
        
        values = []
        for row in posts_data:
            # Extract fields from row
            post_id = row['id']
            text = row['text']
            author = row['author']
            created_at = row['created_at']
            source = row['source']
            sentiment = row['sentiment']
            sentiment_score = row['sentiment_score']
            confidence = row['confidence']
            processed_at = row['processed_at']
            
            # Convert processed_at to datetime if it's a timestamp
            if processed_at:
                if not isinstance(processed_at, datetime):
                    processed_at = datetime.fromtimestamp(processed_at.timestamp())
            else:
                processed_at = datetime.utcnow()
            
            values.append((
                post_id, text, author, created_at, source,
                sentiment, sentiment_score, confidence, processed_at
            ))
        
        if values:
            sql = """
            INSERT INTO sentiment_posts 
            (id, text, author, created_at, source, sentiment, sentiment_score, confidence, processed_at)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
            """
            execute_values(cur, sql, values)
            conn.commit()
            logger.info(f"Batch {batch_id}: Wrote {len(values)} posts to PostgreSQL")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error writing posts to PostgreSQL: {e}")
        # Don't raise - allow streaming to continue even if DB write fails

# Define enhanced schema with validation
input_schema = StructType([
    StructField("text", StringType(), True),
    StructField("author", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("source", StringType(), True)
])

output_schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), False),  # Required field
    StructField("author", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("source", StringType(), True),
    StructField("sentiment", StringType(), False),  # Required field
    StructField("sentiment_score", DoubleType(), False),  # Required field
    StructField("confidence", DoubleType(), True),
    StructField("processed_at", TimestampType(), False)  # Required field
])

def analyze_sentiment(text):
    """
    Analyze sentiment using NLTK VADER.
    Returns: (sentiment_label, compound_score, confidence)
    """
    if not text or not isinstance(text, str):
        return "NEUTRAL", 0.0, 0.0

    # Clean and validate text
    text = text.strip()
    if len(text) == 0 or len(text) > 10000:  # Reasonable limits
        return "NEUTRAL", 0.0, 0.0

    try:
        # Get sentiment scores
        scores = sia.polarity_scores(text)
        compound = scores['compound']

        # Classify sentiment based on compound score
        if compound >= 0.05:
            sentiment = "POSITIVE"
        elif compound <= -0.05:
            sentiment = "NEGATIVE"
        else:
            sentiment = "NEUTRAL"

        # Calculate confidence (absolute value of compound score)
        confidence = abs(compound)

        return sentiment, float(compound), float(confidence)

    except Exception as e:
        logger.warning(f"Sentiment analysis failed for text: {text[:100]}... Error: {e}")
        return "NEUTRAL", 0.0, 0.0

def validate_and_enrich_data(text, author, created_at, source):
    """
    Validate and enrich incoming data.
    Returns: (id, text, author, created_at, source, sentiment, score, confidence)
    """
    import re
    import uuid
    from datetime import datetime

    # Generate unique ID
    post_id = str(uuid.uuid4())

    # Validate and clean text
    if not text or not isinstance(text, str):
        text = ""
    else:
        text = text.strip()
        # Normalize whitespace and remove repeated line breaks
        text = re.sub(r"\s+", " ", text)
        if len(text) == 0 or len(text) > 10000:
            text = ""

    # Validate author
    if not author or not isinstance(author, str) or len(author.strip()) == 0:
        author = "unknown"
    else:
        author = author.strip()

    # Validate created_at
    if not created_at:
        created_at = datetime.utcnow().isoformat()
    elif isinstance(created_at, str):
        created_at = created_at.strip()
        if len(created_at) == 0:
            created_at = datetime.utcnow().isoformat()
    else:
        created_at = str(created_at)

    # Validate source
    if not source or not isinstance(source, str) or len(source.strip()) == 0:
        source = "unknown"
    else:
        source = source.strip().lower()

    # Analyze sentiment
    sentiment, score, confidence = analyze_sentiment(text)

    return post_id, text, author, created_at, source, sentiment, score, confidence

# Register UDFs
validate_enrich_udf = udf(validate_and_enrich_data,
                         StructType([
                             StructField("id", StringType()),
                             StructField("text", StringType()),
                             StructField("author", StringType()),
                             StructField("created_at", StringType()),
                             StructField("source", StringType()),
                             StructField("sentiment", StringType()),
                             StructField("sentiment_score", DoubleType()),
                             StructField("confidence", DoubleType())
                         ]))

# Initialize Spark Session with optimized settings
spark = SparkSession.builder \
    .appName("BlueskySentimentAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

logger.info("Spark session initialized with sentiment analysis capabilities")

# Kafka configuration
kafka_bootstrap = "kafka:9092"
kafka_topics = os.getenv("KAFKA_TOPICS", "bluesky.posts,reddit.posts")

# Read from Kafka topics
# Use comma-separated subscription list for both Bluesky and Reddit topics

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topics) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info(f"Connected to Kafka topics: {kafka_topics}")

# Parse JSON and validate schema
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), input_schema).alias("data")) \
    .select("data.*")

# Apply data validation and sentiment analysis
df_enriched = df_parsed.withColumn("validated_data",
                                  validate_enrich_udf(
                                      col("text"),
                                      col("author"),
                                      col("created_at"),
                                      col("source")
                                  )) \
    .select("validated_data.*") \
    .withColumn("processed_at", current_timestamp())

# Filter out invalid records (empty text)
df_valid = df_enriched.filter(col("text") != "")

# Create source-aware aggregations for real-time analytics
sentiment_counts = df_valid.groupBy("source", "sentiment").count()

# Output sentiment aggregates and posts to PostgreSQL with console display
# This function receives the per-batch DataFrame from df_valid.
def write_to_postgres_and_console(batch_df, batch_id):
    """Write batch data to PostgreSQL and print summary."""
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id}: no records to process")
        return

    # Write detailed posts to PostgreSQL
    write_sentiment_posts_to_postgres(batch_df, batch_id)

    # Aggregate sentiment counts for this batch by source and sentiment
    batch_sentiment_counts = batch_df.groupBy("source", "sentiment").count()
    write_sentiment_aggregates_to_postgres(batch_sentiment_counts, batch_id)

    # Display to console
    print(f"\n{'='*60}")
    print(f"Batch {batch_id} - Sentiment Distribution")
    print(f"{'='*60}")
    batch_sentiment_counts.show(truncate=False)
    print(f"{'='*60}\n")

# Start streaming query with PostgreSQL writes
query = df_valid.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres_and_console) \
    .option("checkpointLocation", "file:///tmp/spark-sentiment-checkpoint") \
    .start()

logger.info("Spark streaming query started - sentiment analysis active")
logger.info("Writing aggregations and posts to PostgreSQL")

try:
    # Wait for any streaming query to terminate; keeps both `query` and `posts_query` active
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Shutting down sentiment analysis pipeline...")
    query.stop()
except Exception as e:
    logger.error(f"Critical error in sentiment analysis pipeline: {e}")
    raise