CREATE TABLE IF NOT EXISTS gold.vendas_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    entity_id VARCHAR(50) NOT NULL,
    avg_value NUMERIC(18, 8),
    min_value NUMERIC(18, 8),
    max_value NUMERIC(18, 8),
    records_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_gold_vendas_daily_date ON gold.vendas_daily(date);
CREATE INDEX IF NOT EXISTS idx_gold_vendas_daily_entity ON gold.vendas_daily(entity_id);