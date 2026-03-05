CREATE SCHEMA IF NOT EXISTS monitoring;

DROP TABLE IF EXISTS monitoring.soda_metricas;

CREATE TABLE monitoring.soda_metricas (
    id SERIAL PRIMARY KEY,
    nome_tabela VARCHAR(500) NOT NULL,
    data_execucao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status_geral VARCHAR(50) NOT NULL,
    taxa_sucesso NUMERIC(5, 2) NOT NULL,
    total_regras INTEGER NOT NULL,
    regras_com_sucesso INTEGER NOT NULL,
    regras_com_falha INTEGER NOT NULL,
    regras_com_alerta INTEGER NOT NULL,
    lista_regras_reprovadas TEXT,
    teve_erro_execucao BOOLEAN NOT NULL DEFAULT FALSE,
    camada VARCHAR(255),
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    data_source VARCHAR(255),
    checks_executados JSONB,
    json_resultado_completo JSONB
);

CREATE INDEX idx_soda_metricas_data_execucao ON monitoring.soda_metricas(data_execucao);
CREATE INDEX idx_soda_metricas_nome_tabela ON monitoring.soda_metricas(nome_tabela);
CREATE INDEX idx_soda_metricas_camada ON monitoring.soda_metricas(camada);
CREATE INDEX idx_soda_metricas_status ON monitoring.soda_metricas(status_geral);
