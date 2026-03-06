CREATE TABLE IF NOT EXISTS bronze.csgostats_raw (
    id SERIAL PRIMARY KEY,
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_csgostats_data ON bronze.csgostats_raw(data_ingestao);