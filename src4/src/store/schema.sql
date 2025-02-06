-- Market Data Schema

-- Create extension for timescaledb
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the market data table
CREATE TABLE IF NOT EXISTS market_data (
    id BIGSERIAL PRIMARY KEY,
    token BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    bid_price DOUBLE PRECISION NOT NULL,
    ask_price DOUBLE PRECISION NOT NULL,
    last_price DOUBLE PRECISION NOT NULL,
    bid_size INTEGER NOT NULL,
    ask_size INTEGER NOT NULL,
    last_size INTEGER NOT NULL,
    sequence_num BIGINT NOT NULL,
    source TEXT NOT NULL,
    message_type TEXT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create hypertable
SELECT create_hypertable('market_data', 'timestamp');

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_market_data_token ON market_data (token);
CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_token_timestamp ON market_data (token, timestamp DESC);

-- Create statistics table
CREATE TABLE IF NOT EXISTS market_stats (
    id BIGSERIAL PRIMARY KEY,
    token BIGINT NOT NULL,
    date DATE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    vwap DOUBLE PRECISION NOT NULL,
    num_trades INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create hypertable for stats
SELECT create_hypertable('market_stats', 'date');

-- Create index on stats
CREATE INDEX IF NOT EXISTS idx_market_stats_token_date ON market_stats (token, date DESC);

-- Create continuous aggregate view for OHLCV
CREATE MATERIALIZED VIEW market_data_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    token,
    first(last_price, timestamp) as open_price,
    max(last_price) as high_price,
    min(last_price) as low_price,
    last(last_price, timestamp) as close_price,
    sum(last_size) as volume,
    count(*) as num_trades
FROM market_data
GROUP BY bucket, token;

-- Add refresh policy
SELECT add_continuous_aggregate_policy('market_data_1min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

-- Create retention policy
SELECT add_retention_policy('market_data',
    INTERVAL '30 days',
    if_not_exists => true);

-- Create compression policy
SELECT add_compression_policy('market_data',
    compress_after => INTERVAL '7 days',
    if_not_exists => true); 