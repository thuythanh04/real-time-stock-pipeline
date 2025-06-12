CREATE TABLE IF NOT EXISTS tick_data (
    symbol TEXT,
    price DOUBLE PRECISION,
    event_time TIMESTAMPTZ NOT NULL,
    volume DOUBLE PRECISION
);
SELECT create_hypertable('tick_data', 'event_time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS ohlcv_data (
    symbol TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    timestamp TIMESTAMPTZ NOT NULL,
    CONSTRAINT ohlcv_unique UNIQUE (timestamp, symbol)
);

SELECT create_hypertable('ohlcv_data', 'timestamp', if_not_exists => TRUE);