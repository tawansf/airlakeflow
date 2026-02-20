-- Gold layer: aggregations and marts for consumption.
CREATE TABLE IF NOT EXISTS gold.bitcoin_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    crypto_id VARCHAR(50) NOT NULL,
    avg_price NUMERIC(18, 8),
    min_price NUMERIC(18, 8),
    max_price NUMERIC(18, 8),
    records_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date, crypto_id)
);

CREATE INDEX IF NOT EXISTS idx_gold_bitcoin_daily_date ON gold.bitcoin_daily(date);
CREATE INDEX IF NOT EXISTS idx_gold_bitcoin_daily_crypto ON gold.bitcoin_daily(crypto_id);
