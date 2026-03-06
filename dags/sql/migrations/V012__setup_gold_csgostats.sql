CREATE TABLE IF NOT EXISTS gold.csgostats_daily (
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

CREATE INDEX IF NOT EXISTS idx_gold_csgostats_daily_date ON gold.csgostats_daily(date);
CREATE INDEX IF NOT EXISTS idx_gold_csgostats_daily_entity ON gold.csgostats_daily(entity_id);