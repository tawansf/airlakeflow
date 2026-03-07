CREATE TABLE IF NOT EXISTS bronze.bitcoin_raw (
    id SERIAL PRIMARY KEY,
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_data ON bronze.bitcoin_raw(data_ingestao);
