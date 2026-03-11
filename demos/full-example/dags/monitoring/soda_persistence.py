import json
import logging
import os
import re
import subprocess
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _to_serializable(val: Any) -> Any:
    """Convert values to JSON-serializable types."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if hasattr(val, "value"):
        return str(val.value) if val.value is not None else str(val)
    if isinstance(val, (Decimal,)):
        return float(val)
    if isinstance(val, dict):
        return {k: _to_serializable(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_to_serializable(v) for v in val]
    if isinstance(val, (str, int, float, bool)):
        return val
    return str(val)


def _parse_dataset_from_contract_file(content: str) -> tuple[Optional[str], Optional[str]]:
    """Extract schema and table from a fully-qualified dataset (data_source/db/schema/table)."""
    match = re.search(r"dataset:\s*[\w\-]+/[\w\-]+/([\w\-]+)/([\w\-]+)", content)
    if match:
        return match.group(1), match.group(2)
    return None, None


def _aggregate_scan_results(
    checks: list[dict[str, Any]],
    has_errors: bool = False,
) -> dict[str, Any]:
    total = len(checks)
    regras_sucesso = sum(1 for c in checks if (c.get("outcome") or "").lower() == "pass")
    regras_falha = sum(1 for c in checks if (c.get("outcome") or "").lower() == "fail")
    regras_alerta = sum(1 for c in checks if (c.get("outcome") or "").lower() == "warn")
    taxa = (regras_sucesso / total * 100.0) if total else 0.0
    if regras_falha > 0 or has_errors:
        status_geral = "Failed"
    elif regras_alerta > 0:
        status_geral = "Passed with Alerts"
    else:
        status_geral = "Passed"
    lista_falhas = [c.get("name") or c.get("check_name") or "Unnamed" for c in checks if (c.get("outcome") or "").lower() == "fail"]
    lista_regras_reprovadas = ", ".join(lista_falhas) if lista_falhas else None
    checks_executados = [{"name": c.get("name") or c.get("check_name"), "outcome": c.get("outcome")} for c in checks]
    return {
        "status_geral": status_geral,
        "taxa_sucesso": round(taxa, 2),
        "total_regras": total,
        "regras_com_sucesso": regras_sucesso,
        "regras_com_falha": regras_falha,
        "regras_com_alerta": regras_alerta,
        "lista_regras_reprovadas": lista_regras_reprovadas,
        "teve_erro_execucao": has_errors,
        "checks_executados": checks_executados,
    }


def _serialize_soda4_result(result: Any) -> dict[str, Any]:
    """Serialize the full result of verify_contract_locally to a JSON-serializable dict."""
    session = {
        "is_ok": getattr(result, "is_ok", None),
        "is_warned": getattr(result, "is_warned", None),
        "is_passed": getattr(result, "is_passed", None),
        "is_failed": getattr(result, "is_failed", None),
        "has_errors": getattr(result, "has_errors", None),
        "has_excluded": getattr(result, "has_excluded", None),
        "number_of_checks": getattr(result, "number_of_checks", None),
        "number_of_checks_passed": getattr(result, "number_of_checks_passed", None),
        "number_of_checks_failed": getattr(result, "number_of_checks_failed", None),
        "number_of_checks_excluded": getattr(result, "number_of_checks_excluded", None),
        "started_timestamp": _to_serializable(getattr(result, "started_timestamp", None)),
        "ended_timestamp": _to_serializable(getattr(result, "ended_timestamp", None)),
        "data_timestamp": _to_serializable(getattr(result, "data_timestamp", None)),
        "errors": result.get_errors() if hasattr(result, "get_errors") and callable(result.get_errors) else [],
        "logs": result.get_logs() if hasattr(result, "get_logs") and callable(result.get_logs) else [],
    }
    contract_results = []
    for cvr in getattr(result, "contract_verification_results", []) or []:
        contract = getattr(cvr, "contract", None)
        contract_info = None
        if contract:
            contract_info = {
                "dataset_id": getattr(contract, "dataset_id", None),
                "dataset_name": getattr(contract, "dataset_name", None),
                "dataset_prefix": _to_serializable(getattr(contract, "dataset_prefix", None)),
                "data_source_name": getattr(contract, "data_source_name", None),
                "soda_qualified_dataset_name": getattr(contract, "soda_qualified_dataset_name", None),
            }
        check_results = []
        for cr in getattr(cvr, "check_results", []) or []:
            check = getattr(cr, "check", None)
            check_info = None
            if check:
                th = getattr(check, "threshold", None)
                check_info = {
                    "type": getattr(check, "type", None),
                    "name": getattr(check, "name", None),
                    "identity": getattr(check, "identity", None),
                    "column_name": getattr(check, "column_name", None),
                    "qualifier": getattr(check, "qualifier", None),
                    "location": getattr(check, "location", None),
                    "threshold": _to_serializable(th) if th is not None else None,
                }
            outcome = getattr(cr, "outcome", None)
            outcome_str = _to_serializable(outcome)
            check_results.append({
                "outcome": outcome_str,
                "diagnostic_metric_values": _to_serializable(getattr(cr, "diagnostic_metric_values", None)),
                "threshold_value": _to_serializable(getattr(cr, "threshold_value", None)),
                "check": check_info,
                "is_passed": getattr(cr, "is_passed", None),
                "is_failed": getattr(cr, "is_failed", None),
                "is_not_evaluated": getattr(cr, "is_not_evaluated", None),
            })
        measurements = []
        for m in getattr(cvr, "measurements", []) or []:
            measurements.append({
                "value": _to_serializable(getattr(m, "value", None)),
                "metric_name": getattr(m, "metric_name", None),
                "metric_id": getattr(m, "metric_id", None),
            })
        status = getattr(cvr, "status", None)
        contract_results.append({
            "status": _to_serializable(status),
            "started_timestamp": _to_serializable(getattr(cvr, "started_timestamp", None)),
            "ended_timestamp": _to_serializable(getattr(cvr, "ended_timestamp", None)),
            "data_timestamp": _to_serializable(getattr(cvr, "data_timestamp", None)),
            "scan_id": getattr(cvr, "scan_id", None),
            "contract": contract_info,
            "check_results": check_results,
            "measurements": measurements,
            "number_of_checks": getattr(cvr, "number_of_checks", None),
            "number_of_checks_passed": getattr(cvr, "number_of_checks_passed", None),
            "number_of_checks_failed": getattr(cvr, "number_of_checks_failed", None),
        })
    return {"session": session, "contract_verification_results": contract_results}


def _insert_soda_metricas_agrupado(
    pg_hook: PostgresHook,
    nome_tabela: str,
    data_execucao: datetime,
    camada: Optional[str],
    dag_id: str,
    task_id: str,
    data_source: str,
    agg: dict[str, Any],
    json_resultado_completo: Optional[dict] = None,
    started_timestamp: Optional[datetime] = None,
    ended_timestamp: Optional[datetime] = None,
    data_timestamp: Optional[datetime] = None,
    contract_status: Optional[str] = None,
    measurements: Optional[list] = None,
) -> None:
    insert_sql = """
        INSERT INTO monitoring.soda_metricas (
            nome_tabela, data_execucao, status_geral, taxa_sucesso,
            total_regras, regras_com_sucesso, regras_com_falha, regras_com_alerta,
            lista_regras_reprovadas, teve_erro_execucao, camada,
            dag_id, task_id, data_source, checks_executados, json_resultado_completo,
            started_timestamp, ended_timestamp, data_timestamp, contract_status, measurements
        ) VALUES (
            %(nome_tabela)s, %(data_execucao)s, %(status_geral)s, %(taxa_sucesso)s,
            %(total_regras)s, %(regras_com_sucesso)s, %(regras_com_falha)s, %(regras_com_alerta)s,
            %(lista_regras_reprovadas)s, %(teve_erro_execucao)s, %(camada)s,
            %(dag_id)s, %(task_id)s, %(data_source)s, %(checks_executados)s, %(json_resultado_completo)s,
            %(started_timestamp)s, %(ended_timestamp)s, %(data_timestamp)s, %(contract_status)s, %(measurements)s
        )
    """
    params = {
        "nome_tabela": nome_tabela,
        "data_execucao": data_execucao,
        "status_geral": agg["status_geral"],
        "taxa_sucesso": agg["taxa_sucesso"],
        "total_regras": agg["total_regras"],
        "regras_com_sucesso": agg["regras_com_sucesso"],
        "regras_com_falha": agg["regras_com_falha"],
        "regras_com_alerta": agg["regras_com_alerta"],
        "lista_regras_reprovadas": agg["lista_regras_reprovadas"],
        "teve_erro_execucao": agg["teve_erro_execucao"],
        "camada": camada,
        "dag_id": dag_id,
        "task_id": task_id,
        "data_source": data_source,
        "checks_executados": json.dumps(agg["checks_executados"], ensure_ascii=False) if agg.get("checks_executados") else None,
        "json_resultado_completo": json.dumps(json_resultado_completo, default=str) if json_resultado_completo else None,
        "started_timestamp": started_timestamp,
        "ended_timestamp": ended_timestamp,
        "data_timestamp": data_timestamp,
        "contract_status": contract_status,
        "measurements": json.dumps(measurements, default=str) if measurements is not None else None,
    }
    pg_hook.run(insert_sql, parameters=params)


def _run_verify_v4(
    data_source_path: str,
    contract_path: str,
) -> tuple[
    list[dict[str, Any]], bool, Optional[dict], int,
    Optional[datetime], Optional[datetime], Optional[datetime], Optional[str], Optional[list],
]:
    """
    Execute verify_contract_locally (Soda 4+).
    Returns (checks_for_agg, has_errors, json_full_result, exit_code,
             started_timestamp, ended_timestamp, data_timestamp, contract_status, measurements).
    """
    try:
        from soda_core.contracts import verify_contract_locally
    except ImportError as e1:
        try:
            from soda.contracts import verify_contract_locally
        except ImportError as e2:
            logger.warning(
                "Soda 4 Python API not available: soda_core.contracts (%s), soda.contracts (%s). "
                "Install soda-core>=4 and soda-postgres>=4 for full API. Falling back to CLI.",
                e1, e2,
            )
            raise

    result = verify_contract_locally(
        data_source_file_path=data_source_path,
        contract_file_path=contract_path,
    )
    exit_code = 0 if result.is_ok else (1 if result.is_warned else 2)
    has_errors = getattr(result, "has_errors", False)
    checks_for_agg = []
    json_resultado_completo = _serialize_soda4_result(result)
    started_ts = getattr(result, "started_timestamp", None)
    ended_ts = getattr(result, "ended_timestamp", None)
    data_ts = getattr(result, "data_timestamp", None)
    contract_status = None
    measurements_out = None
    cvrs = getattr(result, "contract_verification_results", []) or []
    if cvrs:
        first = cvrs[0]
        started_ts = started_ts or getattr(first, "started_timestamp", None)
        ended_ts = ended_ts or getattr(first, "ended_timestamp", None)
        data_ts = data_ts or getattr(first, "data_timestamp", None)
        status = getattr(first, "status", None)
        contract_status = _to_serializable(status) if status is not None else None
        ms = getattr(first, "measurements", None)
        if ms:
            measurements_out = [{"metric_id": getattr(m, "metric_id", None), "metric_name": getattr(m, "metric_name", None), "value": _to_serializable(getattr(m, "value", None))} for m in ms]

    for cvr in cvrs:
        contract = getattr(cvr, "contract", None)
        if contract:
            prefix = getattr(contract, "dataset_prefix", None) or []
            name = getattr(contract, "dataset_name", "") or ""
            if isinstance(prefix, list) and len(prefix) >= 1:
                nome_tabela = f"{prefix[-1]}.{name}"
            else:
                nome_tabela = name
        for cr in getattr(cvr, "check_results", []) or []:
            outcome = getattr(cr, "outcome", None)
            if outcome is not None:
                outcome_str = getattr(outcome, "value", str(outcome)).lower()
                if outcome_str in ("passed", "pass"):
                    outcome_str = "pass"
                elif outcome_str in ("failed", "fail"):
                    outcome_str = "fail"
                elif outcome_str in ("warned", "warn"):
                    outcome_str = "warn"
                else:
                    outcome_str = "pass" if outcome_str == "not_evaluated" else outcome_str
            else:
                outcome_str = "pass"
            check = getattr(cr, "check", None)
            check_name = getattr(check, "name", None) or (getattr(check, "identity", None) if check else None)
            checks_for_agg.append({"name": check_name or "check", "outcome": outcome_str})
    if not checks_for_agg and result:
        checks_for_agg = [{"name": "run", "outcome": "pass" if result.is_ok else "fail"}]
    return checks_for_agg, has_errors, json_resultado_completo, exit_code, started_ts, ended_ts, data_ts, contract_status, measurements_out


def _parse_soda_stdout(stdout: str, stderr: str) -> list[dict[str, Any]]:
    rows = []
    combined = (stdout + "\n" + stderr).strip()
    pass_match = re.search(r"(\d+) check.* passed|passed.*(\d+)", combined, re.I)
    fail_match = re.search(r"(\d+) check.* fail|fail.*(\d+)", combined, re.I)
    passed = int(pass_match.group(1) or pass_match.group(2) or 0) if pass_match else 0
    failed = int(fail_match.group(1) or fail_match.group(2) or 0) if fail_match else 0
    for _ in range(passed):
        rows.append({"outcome": "pass", "measured_value": None, "message": None})
    for _ in range(failed):
        rows.append({"outcome": "fail", "measured_value": None, "message": combined[:5000]})
    if not rows:
        rows.append({
            "outcome": "pass" if "passed" in combined.lower() or "success" in combined.lower() else "fail",
            "measured_value": None,
            "message": combined[:5000] if combined else None,
        })
    return rows


def run_soda_scan_and_persist(
    data_source: str,
    config_path: str,
    contract_path: str,
    dag_id: str,
    task_id: str,
) -> int:
    """
    Execute a Soda 4+ contract verification and persist results into monitoring.soda_metricas.
    contract_path: path to the contract YAML (e.g.: soda/contracts/bitcoin_bronze.yaml).
    config_path: path to the data source YAML (e.g.: soda/configuration.yaml).
    """
    config = config_path if os.path.isabs(config_path) else config_path
    contract = contract_path if os.path.isabs(contract_path) else contract_path

    table_schema, table_name = None, None
    if os.path.isfile(contract):
        with open(contract, "r", encoding="utf-8") as f:
            content = f.read()
            table_schema, table_name = _parse_dataset_from_contract_file(content)
    else:
        content = ""

    executed_at = datetime.now()
    checks_for_agg = []
    scan_results = None
    exit_code = 2
    has_errors = False
    started_ts = ended_ts = data_ts = None
    contract_status = None
    measurements_list = None
    cli_used = False
    stderr = stdout = ""
    try:
        checks_for_agg, has_errors, scan_results, exit_code, started_ts, ended_ts, data_ts, contract_status, measurements_list = _run_verify_v4(
            data_source_path=config,
            contract_path=contract,
        )
    except Exception as e:
        logger.warning("Soda 4 verify_contract_locally failed (%s), falling back to CLI.", e)
        cmd = ["soda", "contract", "verify", "--data-source", config, "--contract", contract]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        exit_code = proc.returncode
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        parsed = _parse_soda_stdout(stdout, stderr)
        checks_for_agg = [{"name": f"check_{i+1}", "outcome": r.get("outcome", "pass")} for i, r in enumerate(parsed)]
        if not checks_for_agg:
            checks_for_agg = [{"name": "run", "outcome": "pass" if exit_code in (0, 1) else "fail"}]
        scan_results = {"checks": checks_for_agg, "hasErrors": exit_code not in (0, 1)}
        cli_used = True
    else:
        stderr = stdout = ""

    nome_tabela = f"{table_schema}.{table_name}" if table_schema and table_name else "unknown"
    camada = table_schema

    agg = _aggregate_scan_results(checks_for_agg, has_errors=scan_results.get("hasErrors", False) if scan_results else False)

    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    _insert_soda_metricas_agrupado(
        pg_hook=pg_hook,
        nome_tabela=nome_tabela,
        data_execucao=executed_at,
        camada=camada,
        dag_id=dag_id,
        task_id=task_id,
        data_source=data_source,
        agg=agg,
        json_resultado_completo=scan_results,
        started_timestamp=started_ts,
        ended_timestamp=ended_ts,
        data_timestamp=data_ts,
        contract_status=contract_status,
        measurements=measurements_list,
    )
    logger.info("Persisted 1 aggregated soda metric row to monitoring.soda_metricas for %s.", nome_tabela)
    if exit_code in (0, 1, 2):
        if exit_code == 2:
            logger.warning("Soda contract had failing checks (exit 2); result persisted. See monitoring.soda_metricas and report.")
        return exit_code
    hint = ""
    if cli_used and ("unknown command" in (stderr or "").lower() or "contract" in (stderr or "").lower() and "not found" in (stderr or "").lower() or not stderr):
        hint = " If the CLI doesn't recognize 'contract verify', install Soda 4 (soda-core>=4, soda-postgres>=4) and rebuild the Airflow image."
    err_msg = "Soda contract verify failed with exit code %s.%s" % (exit_code, hint)
    if stderr:
        err_msg += " stderr: %s" % (stderr[:500] if len(stderr) > 500 else stderr)
    raise RuntimeError(err_msg)
