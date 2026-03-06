# AirLakeFlow

Framework e CLI para criar e rodar pipelines de dados no padrão **Medallion** (Bronze → Silver → Gold) com **Apache Airflow**.

---

## O que o framework faz

- **Inicializa projetos** com a estrutura pronta (DAGs, Soda, Docker Compose, migrations).
- **Gera pipelines ETL** por domínio (bronze, silver, gold, opcionalmente contratos Soda).
- **Cria migrations SQL** versionadas por camada (bronze/silver/gold).
- **Valida** estrutura e ambiente (Docker, arquivos obrigatórios).
- **Controla a aplicação** (subir, parar, reiniciar, logs) via Docker Compose.

Tudo via comando `**alf`** (alias: `airlakeflow`).

---

## Instalação

```bash
pip install -e .
```

Requisitos: **Python 3.10+**. Para rodar os projetos gerados: **Docker** e **Docker Compose**.

### Desenvolvimento do framework (raiz do repo)

Para trabalhar no código do AirLakeFlow na raiz do repositório, crie um venv e instale o pacote em modo editável:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# ou:  .venv\Scripts\activate   # Windows
pip install -e .
alf --version
```

O `.venv` fica na raiz; os exemplos em `demos/` têm seus próprios ambientes (ex.: `demos/full-example/venv`). Para desenvolvimento: `pip install -e ".[dev]"` (inclui pytest, ruff, black). Rodar testes: `pytest tests/`. Lint: `ruff check src tests`. Formatar: `black src tests`.

---

## Uso rápido

```bash
# 1. Criar um novo projeto
alf init meu-projeto
cd meu-projeto

# 2. Criar um pipeline ETL
alf new etl vendas
alf new migration setup_bronze_vendas --dag vendas --layer bronze
# (editar dags/sql/migrations/ e a lógica em dags/vendas/)

# 3. Opcional: adicionar qualidade com Soda
alf add soda --etl vendas

# 4. Validar e subir
alf validate
alf run
```

---

## Comandos

### Projeto


| Comando                              | Descrição                                                                                                                      |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `alf init [nome]`                    | Cria um novo projeto (pasta com dags/, soda/, docker-compose, etc.). Sem nome, usa o diretório atual.                          |
| `alf validate [--project-root PATH]` | Verifica estrutura (dags/, soda/, docker-compose) e Docker (daemon, stack). Use `--no-docker` ou `--no-stack` para restringir. |


### ETL e migrations


| Comando                  | Descrição                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `alf new etl NAME`       | Gera um pipeline ETL (bronze, silver, gold, pipeline.py). Opções: `--contracts`, `--no-gold`, `--source api        |
| `alf new migration NAME` | Cria uma migration SQL (V0XX__nome.sql). Escolha o DAG e a camada (bronze/silver/gold) ou use `--dag` e `--layer`. |


### Qualidade


| Comando                             | Descrição                                                                                                                      |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `alf add soda [--etl NAME | --all]` | Integra Soda: config, contratos e tarefas de scan nos pipelines. Sem opção: modo interativo (lista ETLs + “Projeto completo”). |


### Docker (aplicação)


| Comando                   | Descrição                                                                                                                                            |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `alf run`                 | Sobe a stack em background (`docker compose up -d`). Cria `.env` e `logs/` se não existirem; define AIRFLOW_UID e porta do Postgres quando possível. |
| `alf stop`                | Para os containers.                                                                                                                                  |
| `alf restart`             | Para e sobe de novo.                                                                                                                                 |
| `alf down [--volumes]`    | Derruba a stack (e opcionalmente remove volumes).                                                                                                    |
| `alf logs [-f] [SERVICE]` | Mostra logs dos serviços.                                                                                                                            |
| `alf ps`                  | Lista containers em execução.                                                                                                                        |


Em todos os comandos que atuam sobre um projeto pode-se usar `**--project-root PATH**` (padrão: diretório atual).

---

## Estrutura de um projeto gerado

Após `alf init nome` ou usando um exemplo em `demos/`:

```
nome/
  dags/              # DAGs Airflow (um subdiretório por domínio)
    setup_database.py
    sql/migrations/  # V001__*.sql, V002__*.sql, ...
  soda/              # Config e contratos Soda
    configuration.yaml
    contracts/
  scripts/            # Scripts de infra (ex.: criar DB)
  config/, plugins/, data/, logs/
  docker-compose.yaml
  Dockerfile
  .env.example, .env, requirements.txt
```

---

## Estrutura do repositório (framework)


| Pasta                | Conteúdo                                                                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **src/airlakeflow/** | Código do framework: CLI, templates, skeleton usado por `alf init` (layout [src](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)). |
| **demos/**           | Exemplos: full-example (projeto completo), test-project.                                                                                                          |
| **docs/**            | Documentação de referência (ver [docs/README.md](docs/README.md)).                                                                                                |
| **planning/**        | Documentos de planejamento e design (não são doc de usuário).                                                                                                     |
| **tests/**           | Testes do framework (pytest).                                                                                                                                     |


---

## Licença

[LICENSE](LICENSE).