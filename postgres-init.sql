-- PostgreSQL initialization script for Sentiment Analytics

-- Create sentiment_aggregate table for real-time aggregations
CREATE TABLE IF NOT EXISTS sentiment_aggregate (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) NOT NULL DEFAULT 'unknown',
    sentiment VARCHAR(20) NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    UNIQUE(timestamp, source, sentiment)
);

-- Create sentiment_posts table for detailed post-level analytics
CREATE TABLE IF NOT EXISTS sentiment_posts (
    id VARCHAR(36) PRIMARY KEY,
    text TEXT NOT NULL,
    author VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE,
    source VARCHAR(50) DEFAULT 'bluesky',
    sentiment VARCHAR(20) NOT NULL,
    sentiment_score FLOAT NOT NULL,
    confidence FLOAT,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    batch_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sentiment_aggregate_timestamp ON sentiment_aggregate(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_aggregate_source ON sentiment_aggregate(source);
CREATE INDEX IF NOT EXISTS idx_sentiment_aggregate_sentiment ON sentiment_aggregate(sentiment);
CREATE INDEX IF NOT EXISTS idx_sentiment_posts_source ON sentiment_posts(source);
CREATE INDEX IF NOT EXISTS idx_sentiment_posts_sentiment ON sentiment_posts(sentiment);
CREATE INDEX IF NOT EXISTS idx_sentiment_posts_created_at ON sentiment_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_posts_author ON sentiment_posts(author);

-- Create materialized view for daily statistics
CREATE TABLE IF NOT EXISTS sentiment_daily_stats (
    date DATE NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'unknown',
    sentiment VARCHAR(20) NOT NULL,
    post_count BIGINT NOT NULL DEFAULT 0,
    avg_sentiment_score FLOAT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(date, source, sentiment)
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON sentiment_daily_stats(date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_stats_source ON sentiment_daily_stats(source);
