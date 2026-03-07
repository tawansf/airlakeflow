CREATE TABLE IF NOT EXISTS silver.bitcoin (
    id SERIAL PRIMARY KEY,
    crypto_id VARCHAR(50) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    price NUMERIC(18, 8),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_crypto ON silver.bitcoin(crypto_id);
CREATE INDEX IF NOT EXISTS idx_silver_date ON silver.bitcoin(updated_at);
