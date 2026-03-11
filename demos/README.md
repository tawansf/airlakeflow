# AirLakeFlow Examples

This folder contains demonstration projects for the framework. Each subfolder is an independent AirLakeFlow project.

## Available examples

| Project          | Description                                                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **full-example** | Full pipeline: crypto DAG (Bronze → Silver → Gold), Soda, monitoring. Use as a structure and best-practices reference.       |
| **test-project** | Minimal project for quick tests.                                                                                              |

## How to run an example

1. Install the framework at the **repository root** (once):
  `pip install -e .` (from the `full-example/` folder).
2. Enter the example project folder (each example is a project):
  ```bash
   cd demos/full-example
  ```
3. Use the `alf` commands from there (the project root is `full-example/`, not `demos/`):
  ```bash
   alf doctor
   alf run
  ```

Each project may have its own `.airlakeflow.yaml` and `.env` at the example root (e.g. `demos/full-example/.env`).

---

For framework documentation, installation and commands, see the [main README](../README.md) at the repository root.