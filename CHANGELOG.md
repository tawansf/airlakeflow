# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- (Nothing yet.)

## [0.2.0] - TBD

- Framework and docs in English (CLI, templates, skeleton, demos, planning).
- ALF-Checks: scaffold and DAG (`alf add alf-checks`), config/checks/ layout.
- Partitioned migrations: `@layer(..., partition_by=...)`, Postgres `PARTITION BY RANGE` + default partition.
- Incremental templates: `transformation.py.j2` uses Airflow Variable for `--incremental-by` (pandas path).
- CI: GitHub Actions (ruff, black, pytest, bandit, pip-audit, wheel build; Python 3.10–3.12).
- Docs: CHANGELOG, CONTRIBUTING, issue/PR templates.
- Tests: new_etl, data_tests_cmd, cli add alf-checks, migration_gen (including partitioned).
- Ruff/Black fixes and test formatting.

