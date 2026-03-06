# Exemplos AirLakeFlow

Esta pasta contém **projetos de demonstração** do framework. Cada subpasta é um projeto AirLakeFlow independente.

## Exemplos disponíveis


| Projeto          | Descrição                                                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **full-example** | Pipeline completo: DAG crypto (Bronze → Silver → Gold), Soda, monitoring. Use como referência de estrutura e boas práticas. |
| **test-project** | Projeto mínimo para testes rápidos.                                                                                         |


## Como rodar um exemplo

1. Instale o framework na **raiz do repositório** (uma vez):
  `pip install -e .` (na pasta `full-example/`).
2. Entre na pasta do projeto (cada exemplo é um projeto):
  ```bash
   cd demos/full-example
  ```
3. Use os comandos `alf` a partir daí (a raiz do projeto é `full-example/`, não `demos/`):
  ```bash
   alf doctor
   alf run
  ```

Cada projeto pode ter seu próprio `.airlakeflow.yaml` e `.env` na raiz da subpasta (ex.: `demos/full-example/.env`).

---

Para documentação do framework, instalação e comandos, veja o [README principal](../README.md) na raiz do repositório.