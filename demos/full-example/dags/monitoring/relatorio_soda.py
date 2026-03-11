import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

QUERY_ULTIMA_EXECUCAO_POR_TABELA = """
SELECT nome_tabela, camada, data_execucao, status_geral, taxa_sucesso,
       regras_com_sucesso, total_regras, checks_executados,
       started_timestamp, ended_timestamp, data_timestamp, contract_status, measurements, json_resultado_completo
FROM monitoring.soda_metricas m
WHERE (m.nome_tabela, m.data_execucao) IN (
    SELECT nome_tabela, MAX(data_execucao)
    FROM monitoring.soda_metricas
    GROUP BY nome_tabela
)
ORDER BY camada, nome_tabela;
"""

QUERY_TENDENCIA_14_DIAS = """
SELECT date(data_execucao) AS dia, status_geral, count(*) AS qtd
FROM monitoring.soda_metricas
WHERE data_execucao >= current_date - interval '14 days'
GROUP BY date(data_execucao), status_geral
ORDER BY dia;
"""

QUERY_ULTIMOS_7_SCANS_POR_TABELA = """
WITH ranked AS (
    SELECT nome_tabela, data_execucao, taxa_sucesso,
           ROW_NUMBER() OVER (PARTITION BY nome_tabela ORDER BY data_execucao DESC) AS rn
    FROM monitoring.soda_metricas
)
SELECT nome_tabela, data_execucao, taxa_sucesso
FROM ranked
WHERE rn <= 7
ORDER BY nome_tabela, data_execucao ASC;
"""


def _build_resumo(detalhes: list[dict]) -> dict:
    total_aprovados = sum(1 for d in detalhes if d.get("status_geral") in ("Aprovado", "Passed"))
    total_reprovados = sum(1 for d in detalhes if d.get("status_geral") in ("Reprovado", "Failed"))
    total_alertas = sum(1 for d in detalhes if d.get("status_geral") in ("Aprovado com Alertas", "Passed with Alerts"))
    total_tabelas = len(detalhes)
    # Health Score: % of tables without failure (passed or with alerts); 100% if there are no tables
    health_score = (
        round((total_tabelas - total_reprovados) / total_tabelas * 100, 1)
        if total_tabelas
        else 100.0
    )
    return {
        "health_score": health_score,
        "total_aprovados": total_aprovados,
        "total_reprovados": total_reprovados,
        "tabelas_em_falha": total_reprovados,
        "total_alertas": total_alertas,
        "total_tabelas": total_tabelas,
    }


def _build_dados_grafico(detalhes: list[dict]) -> dict:
    by_camada = {}
    if not detalhes:
        return {"labels": [], "datasets": []}
    for d in detalhes:
        camada = (d.get("camada") or "outros").lower()
        if camada not in by_camada:
            by_camada[camada] = {"aprovados": 0, "reprovados": 0, "alertas": 0}
        s = d.get("status_geral")
        if s in ("Aprovado", "Passed"):
            by_camada[camada]["aprovados"] += 1
        elif s in ("Reprovado", "Failed"):
            by_camada[camada]["reprovados"] += 1
        else:
            by_camada[camada]["alertas"] += 1
    labels = sorted(by_camada.keys())
    return {
        "labels": labels,
        "datasets": [
            {"label": "Passed", "data": [by_camada[l]["aprovados"] for l in labels], "backgroundColor": "rgb(16, 185, 129)"},
            {"label": "Failed", "data": [by_camada[l]["reprovados"] for l in labels], "backgroundColor": "rgb(239, 68, 68)"},
            {"label": "Alerts", "data": [by_camada[l]["alertas"] for l in labels], "backgroundColor": "rgb(245, 158, 11)"},
        ],
    }


def _build_sparkline_por_tabela(raw: list[tuple]) -> dict:
    """For each table name, build { labels: [dates], data: [success_rate] } (chronological order)."""
    from collections import defaultdict
    by_table = defaultdict(list)
    for r in raw:
        nome_tabela, data_execucao, taxa_sucesso = r[0], r[1], r[2]
        label = data_execucao.strftime("%d/%m %Hh") if hasattr(data_execucao, "strftime") else str(data_execucao)
        by_table[nome_tabela].append({"label": label, "value": float(taxa_sucesso) if taxa_sucesso is not None else 0})
    return {
        nome: {"labels": [x["label"] for x in vals], "data": [x["value"] for x in vals]}
        for nome, vals in by_table.items()
    }


def _build_dados_grafico_tendencia(raw_tendencia: list[tuple]) -> dict:
    from collections import defaultdict
    if not raw_tendencia:
        return {"labels": [], "datasets": []}
    by_dia = defaultdict(lambda: {"Passed": 0, "Failed": 0, "Passed with Alerts": 0})
    for r in raw_tendencia:
        dia, status_geral, qtd = r[0], r[1], r[2]
        by_dia[dia][status_geral] = qtd
    dias_ordem = sorted(by_dia.keys())
    labels = [d.strftime("%d/%m") if hasattr(d, "strftime") else str(d) for d in dias_ordem]
    return {
        "labels": labels,
        "datasets": [
            {"label": "Passed", "data": [by_dia[d]["Passed"] for d in dias_ordem], "borderColor": "rgb(16, 185, 129)", "fill": False},
            {"label": "Failed", "data": [by_dia[d]["Failed"] for d in dias_ordem], "borderColor": "rgb(239, 68, 68)", "fill": False},
            {"label": "Alerts", "data": [by_dia[d]["Passed with Alerts"] for d in dias_ordem], "borderColor": "rgb(245, 158, 11)", "fill": False},
        ],
    }


def gerar_relatorio_soda(
    conn_id: str = "postgres_datawarehouse",
    output_path: str | None = None,
    titulo: str = "Soda Quality Report",
) -> str:
    if output_path is None:
        base = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = os.path.join(base, "data", "reports", f"relatorio_soda_{ts}.html")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    template_dir = Path(__file__).resolve().parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)))
    template = env.get_template("relatorio_soda.html.j2")

    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        raw = pg_hook.get_records(QUERY_ULTIMA_EXECUCAO_POR_TABELA)
        columns = [
            "nome_tabela", "camada", "data_execucao", "status_geral", "taxa_sucesso",
            "regras_com_sucesso", "total_regras", "checks_executados",
            "started_timestamp", "ended_timestamp", "data_timestamp", "contract_status", "measurements", "json_resultado_completo",
        ]
        detalhes = []
        for r in raw:
            row = dict(zip(columns, r))
            key_tabela = row.get("nome_tabela")
            dt = row.get("data_execucao")
            row["data_execucao"] = dt.strftime("%Y-%m-%d %H:%M") if dt and hasattr(dt, "strftime") else (str(dt) if dt else "–")
            row["nome_tabela"] = key_tabela or "–"
            row["camada"] = row.get("camada") or "–"
            row["status_geral"] = row.get("status_geral") or "–"
            row["taxa_sucesso"] = row.get("taxa_sucesso") if row.get("taxa_sucesso") is not None else 0
            row["regras_com_sucesso"] = row.get("regras_com_sucesso") if row.get("regras_com_sucesso") is not None else 0
            row["total_regras"] = row.get("total_regras") if row.get("total_regras") is not None else 0
            try:
                row["checks_list"] = json.loads(row["checks_executados"]) if row.get("checks_executados") else []
            except (TypeError, json.JSONDecodeError):
                row["checks_list"] = []
            row["contract_status"] = row.get("contract_status") or "–"
            row["measurements"] = []
            row["timestamps"] = {}
            if row.get("started_timestamp") and hasattr(row["started_timestamp"], "strftime"):
                row["timestamps"]["started"] = row["started_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                row["timestamps"]["started"] = row.get("started_timestamp") or "–"
            if row.get("ended_timestamp") and hasattr(row["ended_timestamp"], "strftime"):
                row["timestamps"]["ended"] = row["ended_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                row["timestamps"]["ended"] = row.get("ended_timestamp") or "–"
            if row.get("data_timestamp") and hasattr(row["data_timestamp"], "strftime"):
                row["timestamps"]["data"] = row["data_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                row["timestamps"]["data"] = row.get("data_timestamp") or "–"
            try:
                if row.get("measurements"):
                    row["measurements"] = json.loads(row["measurements"]) if isinstance(row["measurements"], str) else (row["measurements"] or [])
            except (TypeError, json.JSONDecodeError):
                row["measurements"] = []
            full = row.get("json_resultado_completo")
            if isinstance(full, str):
                try:
                    full = json.loads(full)
                except (TypeError, json.JSONDecodeError):
                    full = None
            if full and isinstance(full, dict) and full.get("contract_verification_results"):
                cvrs = full["contract_verification_results"]
                if cvrs and cvrs[0].get("check_results"):
                    enriched = []
                    for cr in cvrs[0]["check_results"]:
                        check_info = cr.get("check") or {}
                        enriched.append({
                            "name": check_info.get("name") or cr.get("outcome") or "–",
                            "outcome": (cr.get("outcome") or "pass").lower().replace("passed", "pass").replace("failed", "fail").replace("warned", "warn"),
                            "check_type": check_info.get("type") or "–",
                            "column_name": check_info.get("column_name") or "–",
                            "diagnostic_metric_values": cr.get("diagnostic_metric_values") or {},
                            "threshold_value": cr.get("threshold_value"),
                        })
                    row["checks_list"] = enriched
                if cvrs and cvrs[0].get("measurements") and not row["measurements"]:
                    row["measurements"] = cvrs[0]["measurements"]
            detalhes.append(row)

        raw_sparklines = pg_hook.get_records(QUERY_ULTIMOS_7_SCANS_POR_TABELA)
        sparkline_por_tabela = _build_sparkline_por_tabela(raw_sparklines)
        for row in detalhes:
            key = row["nome_tabela"] if row.get("nome_tabela") != "–" else None
            row["sparkline"] = sparkline_por_tabela.get(key, {"labels": [], "data": []}) if key else {"labels": [], "data": []}
        resumo = _build_resumo(detalhes)
        dados_grafico = _build_dados_grafico(detalhes)

        raw_tendencia = pg_hook.get_records(QUERY_TENDENCIA_14_DIAS)
        dados_grafico_tendencia = _build_dados_grafico_tendencia(raw_tendencia)

        html = template.render(
            titulo=titulo,
            data_geracao=datetime.now().strftime("%Y-%m-%d %H:%M"),
            resumo=resumo,
            detalhes=detalhes,
            dados_grafico=dados_grafico,
            dados_grafico_tendencia=dados_grafico_tendencia,
            erro=None,
        )
    except Exception as e:
        logger.exception("Error generating Soda report")
        html = template.render(
            titulo=titulo,
            data_geracao=datetime.now().strftime("%Y-%m-%d %H:%M"),
            resumo={"health_score": 100.0, "total_aprovados": 0, "total_reprovados": 0, "tabelas_em_falha": 0, "total_alertas": 0, "total_tabelas": 0},
            detalhes=[],
            dados_grafico={"labels": [], "datasets": []},
            dados_grafico_tendencia={"labels": [], "datasets": []},
            erro=str(e),
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("Soda report generated: %s", output_path)
    return output_path


def _task_gerar_relatorio_soda():
    output_path = os.getenv("SODA_REPORT_PATH")
    gerar_relatorio_soda(output_path=output_path)


default_args_relatorio = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="relatorio_soda_metricas",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    default_args=default_args_relatorio,
    tags=["monitoring", "report", "soda"],
) as dag:
    gerar_relatorio = PythonOperator(
        task_id="gerar_relatorio_soda_html",
        python_callable=_task_gerar_relatorio_soda,
    )
    gerar_relatorio
