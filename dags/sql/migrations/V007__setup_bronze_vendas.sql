CREATE TABLE IF NOT EXISTS bronze.vendas_raw (
    id SERIAL PRIMARY KEY,
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_vendas_data ON bronze.vendas_raw(data_ingestao);