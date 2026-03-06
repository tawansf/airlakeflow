CREATE TABLE IF NOT EXISTS silver.vendas (
    id SERIAL PRIMARY KEY,
    entity_id VARCHAR(50) NOT NULL,
    value NUMERIC(18, 8),
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_vendas_entity ON silver.vendas(entity_id);
CREATE INDEX IF NOT EXISTS idx_silver_vendas_date ON silver.vendas(updated_at);