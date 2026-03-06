# Design: `alf add` (ferramentas de qualidade)

Objetivo: comando **`alf add <ferramenta>`** para “pluguar” ferramentas de qualidade (Soda, Great Expectations, etc.) no projeto, escolhendo em qual ETL vincular ou se aplica ao projeto completo.

---

## 1. Fluxo geral

```
alf add soda
  → Descobre ETLs (dags/*/pipeline.py)
  → Lista opções:
      [1] crypto
      [2] vendas
      [3] csgostats
      [4] Projeto completo
  → Usuário escolhe (número ou nome)
  → Executa a integração da ferramenta na escolha
```

- **Escolha “Projeto completo”:** aplica a integração em **todos** os ETLs listados (ex.: adiciona tarefas Soda em todos os pipelines que ainda não tiverem).
- **Escolha um ETL:** aplica só naquele (ex.: só no `vendas`).

---

## 2. `alf add soda`

### 2.1 O que a ferramenta Soda já usa hoje

- **Config:** `soda/configuration.yaml` (data source Postgres, etc.)
- **Contratos:** `soda/contracts/<entidade>_bronze.yaml`, `<entidade>_silver.yaml`
- **Código:** `dags/monitoring/soda_persistence.py` (run_soda_scan_and_persist)
- **Pipeline:** em cada DAG, tarefas que chamam `run_soda_scan_and_persist` com `contract_path` apontando para o contrato daquele ETL (ex.: bronze + silver do bitcoin, vendas, etc.)

### 2.2 O que `alf add soda` deve fazer

| Escopo        | Ação |
|---------------|------|
| **Projeto**   | Garantir que existem: `soda/configuration.yaml` (se não existir, criar esqueleto), pasta `soda/contracts/`, e módulo `dags/monitoring/soda_persistence.py` (se não existir, copiar do framework). Não alterar pipelines individuais. |
| **Um ETL**    | Para o ETL escolhido: (1) garantir config e monitoring acima se faltarem; (2) se o pipeline **não** tiver tarefas Soda, **adicionar** tarefas de scan (bronze + silver) e dependências (ingestion >> soda_bronze >> transformation >> soda_silver >> gold); (3) se não existirem contratos para esse ETL, criar esqueletos `soda/contracts/<etl>_bronze.yaml` e `<etl>_silver.yaml` (Soda 4, placeholders). |
| **Projeto completo** | Para **cada** ETL da lista: fazer o mesmo que “Um ETL” (adicionar tarefas Soda e contratos só nos que ainda não tiverem). |

### 2.3 Detalhes importantes

- **Não sobrescrever:** Se o pipeline já tiver tarefas Soda (ex.: já tem `soda_scan_bronze_*`), não duplicar. Se já existir `configuration.yaml` ou contrato, não sobrescrever; no máximo avisar “Soda já configurado para X”.
- **Nome da entidade:** Usar o nome da pasta do ETL como entidade (ex.: `vendas` → tabelas `vendas_raw`, `vendas`; contratos `vendas_bronze.yaml`, `vendas_silver.yaml`). O pipeline já usa esse padrão; o add só precisa seguir o mesmo.
- **Ordem no DAG:** Inserir tarefas Soda no fluxo existente: após ingestão bronze → soda bronze; após silver → soda silver; antes do gold. Se o DAG não tiver gold, encadear até soda_silver.
- **Contratos esqueleto:** Conteúdo mínimo (dataset, checks básicos row_count, uma coluna payload/updated_at) para o Soda rodar; o usuário edita depois.

### 2.4 Opções de comando (sugestão)

- `alf add soda` — interativo (lista ETLs + projeto completo, pergunta).
- `alf add soda --etl vendas` — não pergunta; aplica só no ETL `vendas`.
- `alf add soda --all` — aplica em todos os ETLs sem perguntar.

---

## 3. `alf add greatxp` (futuro)

- **Objetivo:** Integrar Great Expectations (ou outra ferramenta) da mesma forma: escolher ETL(s) ou projeto completo e “pluguar” (config, expectations, tarefas nos DAGs).
- **Design detalhado:** Fica para uma segunda fase, quando for implementar. O fluxo de `alf add` (listar ETLs, escolher um ou projeto completo) pode ser o mesmo; só muda o que é criado/alterado (arquivos e tarefas específicos do Great Expectations).

---

## 4. Estrutura do comando na CLI

```
alf add soda [--etl NAME | --all] [--project-root PATH]
alf add greatxp [--etl NAME | --all] [--project-root PATH]
```

- Sem `--etl` nem `--all`: modo interativo (lista ETLs + “Projeto completo”, pergunta).
- Com `--etl NAME`: aplica só no ETL `NAME`.
- Com `--all`: aplica em todos os ETLs (equivalente a escolher “Projeto completo” no interativo para a ferramenta em questão).

---

## 5. Casos de borda

| Caso | Comportamento |
|------|----------------|
| Nenhum ETL encontrado | Mensagem: “Nenhum ETL em dags/. Crie um com 'alf new etl NAME'.” e sair. |
| Usuário escolhe “Projeto completo” e não há config Soda | Criar config + monitoring uma vez; em seguida, para cada ETL, adicionar tarefas e contratos. |
| Pipeline do ETL não segue o padrão (ex.: sem bronze.py) | Se não conseguir detectar onde encaixar Soda, avisar e pular aquele ETL ou só criar contratos. |
| Contrato já existe para aquele ETL | Não sobrescrever; avisar “Contrato vendas_bronze já existe; nada alterado.” |

---

## 6. Resumo para implementar primeiro

1. **Grupo `alf add`** com subcomando **`soda`**.
2. **Descoberta de ETLs** (reutilizar `discover_dags` de `new_migration`).
3. **Modo interativo:** listar ETLs + “Projeto completo”, ler escolha (número ou nome).
4. **Modos diretos:** `--etl NAME` e `--all`.
5. **Ações Soda (por ETL):** garantir config/monitoring no projeto; no pipeline do ETL, inserir tarefas Soda e dependências; criar contratos esqueleto se não existirem.
6. **Não sobrescrever** config, contratos ou tarefas já existentes.

Quando esse desenho estiver ok, o próximo passo é implementar só o **`alf add soda`** conforme acima; **`alf add greatxp`** pode ficar como stub (“em desenvolvimento”) até você definir o que exatamente plugar.
