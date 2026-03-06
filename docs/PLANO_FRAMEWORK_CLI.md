# Plano: Framework de Data Engineering + CLI

## Visão geral

Transformar o repositório atual em um **framework** com **CLI** que:
- **init**: cria um novo projeto com a estrutura padrão (e opcionalmente DAG demo).
- **new-etl**: gera um novo pipeline ETL (bronze → silver → gold) com flags (contratos Soda, tipo de fonte, etc.).

O repositório atual serve como **template de referência** e contém a DAG demo (crypto). A CLI pode viver dentro deste repo (como módulo `cli/` ou pacote instalável) ou em um repo separado que usa este como template.

---

## 1. Separação: Framework vs Projeto gerado

| Conceito | O que é |
|----------|--------|
| **Framework** | Código reutilizável: estrutura de pastas, convenções, módulos compartilhados (`soda_persistence`, `relatorio_soda`, `setup_database`), templates de DAG/contratos/migrations. |
| **Projeto gerado** | O que o usuário obtém ao rodar `init` ou `new-etl`: um diretório com `dags/`, `soda/`, `docker-compose`, etc., preenchido conforme as escolhas. |

**Recomendação:** Este repo **é** o framework. Você adiciona um pacote CLI (ex.: `airlakeflow`) aqui mesmo:

- `airlakeflow/` com entrada da CLI (Click/Typer).
- `templates/` ou `framework_templates/` com arquivos base (pipeline.py.j2, bronze.yaml.j2, etc.).
- Comando `init` **copia** a estrutura deste repo para um novo diretório (ou gera a partir de templates), com flag `--no-demo` para não incluir a pasta `crypto`.
- Comando `new-etl` **gera** arquivos dentro do projeto atual (ou no path passado).

Assim, **init** = “criar novo projeto a partir deste template”. **new-etl** = “adicionar um novo pipeline ao projeto atual”.

---

## 2. Comandos da CLI

### 2.1 `init [destino]`

Cria um novo projeto de data engineering.

- **destino**: pasta do novo projeto (default: `.` ou `./my-data-project`).
- **Flags sugeridas**:
  - `--demo` / `--no-demo`: incluir ou não a DAG demo (crypto). Default: `--demo`.
  - `--with-monitoring`: incluir schema monitoring, relatório Soda e DAG de relatório (default: true).
  - `--airflow-only`: só estrutura Airflow + migrations mínimas, sem Soda/relatório.

**O que gera:**
- Estrutura de pastas: `dags/`, `dags/sql/migrations/`, `dags/monitoring/`, `soda/`, `soda/contracts/`, `scripts/`, `config/`, `plugins/`, `data/`.
- Arquivos base: `docker-compose.yaml`, `Dockerfile`, `.env.example`, `requirements.txt`, `README.md`.
- Migrations iniciais: V001 (schemas), V002–V004 (bronze/silver/gold de exemplo se demo), V005–V006 (monitoring se --with-monitoring).
- Se `--demo`: `dags/crypto/` + `soda/contracts/bitcoin_*.yaml` + pipeline crypto.
- Módulos compartilhados: `dags/monitoring/soda_persistence.py`, `relatorio_soda.py`, `setup_database.py` (copiados do framework).

### 2.2 `new-etl <nome> [opções]`

Gera um novo pipeline ETL no projeto **atual** (precisa ser executado dentro de um projeto já inicializado).

- **nome**: nome do domínio do pipeline (ex.: `vendas`, `users`, `weather`). Será usado em `dags/<nome>/`, tabelas `<nome>_raw`, `<nome>`, etc.
- **Flags sugeridas**:
  - `--contracts`: gera contratos Soda (bronze + silver) e adiciona tarefas `soda_scan_bronze_*` e `soda_scan_silver_*` no DAG.
  - `--gold` / `--no-gold`: incluir camada gold (default: true).
  - `--source`: tipo de ingestão bronze: `api` | `file` | `jdbc` (default: `api`). Altera o esqueleto de `bronze.py`.
  - `--table-name`: nome da tabela (default: igual ao `nome`). Útil se quiser `dags/vendas/` mas tabela `vendas_pedidos`.
  - `--no-spark`: silver sem Spark (apenas Python/SQL). Gera `silver.py` sem dependência de PySpark.

**O que gera:**
- `dags/<nome>/pipeline.py` – DAG com dependências bronze → soda_bronze → silver → soda_silver → gold (se --gold).
- `dags/<nome>/bronze.py` – ingestão (esqueleto conforme --source).
- `dags/<nome>/silver.py` – chama transformação (Spark ou não).
- `dags/<nome>/gold.py` – agregação (se --gold).
- `dags/<nome>/transformations/<nome>.py` – lógica de transformação (placeholder).
- Migrations SQL: novas versões para `bronze.<table>_raw`, `silver.<table>`, `gold.<table>_daily` (ou similar). Numeração: próximo V00X disponível.
- Se `--contracts`: `soda/contracts/<table>_bronze.yaml` e `soda/contracts/<table>_silver.yaml` (esqueleto Soda 4).
- Atualização de `dags/setup_database.py`: registrar novas migrations (ou usar descoberta automática por nome de arquivo).

### 2.3 Comandos auxiliares (fase 2)

- `new-contract <schema> <table>` – gera apenas um contrato Soda (bronze ou silver) para uma tabela existente.
- `report` – gera o relatório HTML de Soda a partir de `monitoring.soda_metricas` (chama a lógica de `relatorio_soda.py`).
- `list-etls` – lista pipelines (pastas em `dags/` que contêm `pipeline.py`).

---

## 3. Convenções que a CLI deve respeitar

- **DAGs**: um arquivo `pipeline.py` por domínio em `dags/<dominio>/`.
- **Migrations**: `dags/sql/migrations/VNNN__descricao.sql`; ordem por número.
- **Contratos Soda**: `soda/contracts/<tabela>_<camada>.yaml` (ex.: `bitcoin_bronze.yaml`, `bitcoin_silver.yaml`).
- **Nome de tarefas**: `bronze_ingestion_data_<entidade>`, `soda_scan_bronze_<entidade>`, `silver_transformation_data_<entidade>`, `soda_scan_silver_<entidade>`, `gold_aggregate_<entidade>_daily`.
- **Conexão Airflow**: `postgres_datawarehouse` para o data warehouse.
- **Soda**: `soda/configuration.yaml` com data source; paths em `SODA_PATH` (env).

---

## 4. Implementação técnica sugerida

1. **Pacote Python**
   - `pyproject.toml` com `[project.scripts]` apontando para a CLI (ex.: `airlakeflow = airlakeflow.cli:main`).
   - Dependências: `click` ou `typer`, `jinja2`, `pyyaml`. Opcional: `cookiecutter` se quiser usar cookiecutter para init.

2. **Templates**
   - Usar **Jinja2** para arquivos gerados: `pipeline.py.j2`, `bronze.py.j2`, `contract_bronze.yaml.j2`, etc.
   - Variáveis: `nome`, `table_name`, `with_contracts`, `with_gold`, `source_type`, `use_spark`.

3. **Init**
   - Copiar árvore de arquivos do framework (excluindo `.git`, `logs`, `__pycache__`, dados sensíveis) para o destino, ou
   - Gerar a partir de uma pasta `framework_templates/init/` com placeholders; substituir nomes e remover o que for `--no-demo` / `--no-monitoring`.

4. **New-ETL**
   - Descobrir o próximo número de migration (listar `dags/sql/migrations/V*.sql` e pegar max+1).
   - Gerar arquivos a partir de templates Jinja2.
   - Para `setup_database.py`: ou manter uma lista explícita de migrations e appendar, ou passar a usar descoberta por glob e ordem por nome (recomendado para framework).

5. **Descoberta de migrations**
   - Em `setup_database.py`, em vez de listar V001…V006 manualmente, fazer `sorted(glob("dags/sql/migrations/V*.sql"))` e executar em ordem. Assim o CLI só precisa criar novos arquivos V00X.

---

## 5. Ordem de implementação sugerida

1. **Descoberta de migrations** em `setup_database.py` (glob + sort) para não quebrar ao adicionar novas migrations pelo CLI.
2. **Pacote CLI**: `pyproject.toml`, estrutura `airlakeflow/`, comando `new-etl` com templates Jinja2 (foco em um pipeline simples: api + bronze + silver + gold + contracts).
3. **Comando `init`**: template mínimo do projeto (pastas + docker-compose + .env.example + migrations base); flag `--demo` para copiar também `dags/crypto` e contratos bitcoin.
4. **Flags extras** de `new-etl`: `--source`, `--no-spark`, `--table-name`.
5. **Comandos auxiliares**: `new-contract`, `report`, `list-etls`.

---

## 6. Resumo: init vs new-etl

| Comando | Uso típico | O que faz |
|---------|------------|-----------|
| **init** | Primeira vez: “criar projeto novo” | Cria pasta do projeto com estrutura completa; opcionalmente inclui DAG demo (crypto). |
| **new-etl** | Dentro do projeto: “adicionar pipeline de vendas com contratos” | Gera `dags/vendas/`, migrations, contratos Soda, e encaixa no padrão bronze → soda → silver → soda → gold. |

**Init não é obrigatório** se você sempre trabalhar em cima deste repo: aí o “projeto” é este repo, e você só usa `new-etl` para adicionar pipelines. Init é recomendado quando quiser entregar um “kit” para outras pessoas ou novos projetos sem clonar o framework inteiro.
