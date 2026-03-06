CREATE TABLE IF NOT EXISTS silver.csgostats (
    id SERIAL PRIMARY KEY,
    entity_id VARCHAR(50) NOT NULL,
    value NUMERIC(18, 8),
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_csgostats_entity ON silver.csgostats(entity_id);
CREATE INDEX IF NOT EXISTS idx_silver_csgostats_date ON silver.csgostats(updated_at);