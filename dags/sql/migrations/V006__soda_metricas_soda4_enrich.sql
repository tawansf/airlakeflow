-- Soda 4: colunas adicionais para timestamps, status do contrato e métricas (nullable para compatibilidade)
ALTER TABLE monitoring.soda_metricas ADD COLUMN IF NOT EXISTS started_timestamp TIMESTAMP;
ALTER TABLE monitoring.soda_metricas ADD COLUMN IF NOT EXISTS ended_timestamp TIMESTAMP;
ALTER TABLE monitoring.soda_metricas ADD COLUMN IF NOT EXISTS data_timestamp TIMESTAMP;
ALTER TABLE monitoring.soda_metricas ADD COLUMN IF NOT EXISTS contract_status VARCHAR(50);
ALTER TABLE monitoring.soda_metricas ADD COLUMN IF NOT EXISTS measurements JSONB;
