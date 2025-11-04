from __future__ import annotations

from datetime import timedelta
import os
import json
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from s3_benchmark.presets import EXAMPLES
from s3_benchmark.config import COMMON_DOCKER, DEFAULT_CONF
from s3_benchmark.commands import (
    bronze_cmd,
    silver_cmd,
    silver_join_cmd,
    export_cmd,
    dq_silver_cmd,
    dq_join_cmd,
)
from s3_benchmark.utils import (
    log_step_metrics,
    short_if_exists,
    _as_bool,
    _get_run_conf,
    _metrics_dir_airflow,
)
from s3_benchmark.dwh import (
    load_fact_single,
    load_fact_parallel,
    dwh_single_enabled,
    dwh_parallel_enabled,
)
from s3_benchmark.dq import (
    store_dq_file,
    dq_dwh_checks,
    store_dq_dwh,
)

# ====== Gate helper ======
def dwh_load_enabled(**context) -> bool:
    """Top-level gate: enable/disable DWH load stage based on run config."""
    conf = _get_run_conf(context)
    return _as_bool(conf.get("dwh_load", False))

# ====== SLA / Alerts ======
def on_fail_callback(context):
    """Simple console alert for demo/defense. In real life: send Slack/Email."""
    ti = context["ti"]
    conf = _get_run_conf(context)
    print(f"[ALERT] Task failed: {ti.dag_id}.{ti.task_id} | run_id={ti.run_id} | exp={conf.get('experiment_id')}")

default_args = {
    "owner": "de",
    "retries": 0,
    "sla": timedelta(minutes=60),
    "on_failure_callback": on_fail_callback,
}

with DAG(
    dag_id="s3_benchmark",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["s3", "optimized", "benchmark"],
    default_args=default_args,
) as dag:

    # --------- DOC (visible in UI) ----------
    dag.doc_md = """
    # S3 Benchmark (Bronze → Silver → Export → DWH)

    **Goal:** compare export & DWH load strategies: `single` vs `parallel`.

    **Key params:** `year`, `month`, `experiment_id`, `export_strategy`, `dwh_load`, `dwh_parallel.enabled`.

    ## Groups
    - **checks** — validate required Spark job files exist
    - **spark_etl** — bronze → silver → join → export + DQ (silver/join)
    - **dwh_load** — two branches: single / parallel (ShortCircuit)
    - **dq_postload** — DQ checks after DWH load
    """

    # Quick examples for UI (Params tab)
    dag.params["default_conf"] = DEFAULT_CONF
    dag.params["examples"] = EXAMPLES

    # ---- Cleanup artifacts from previous run (idempotent) ----
    cleanup_cmd = r"""
    {% set RC  = dag_run.conf if dag_run else {} %}
    {% set DC  = RC.get('default_conf', {}) %}
    {% set CLEAN = 1 if (RC.get('cleanup', DC.get('cleanup', params.default_conf.cleanup))) else 0 %}
    {% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
    {% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
    {% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
    {% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}

    /bin/sh -lc '
    set -e
    if [ "{{ CLEAN }}" != "1" ]; then
      echo "[cleanup] disabled -> skip"
      exit 0
    fi
    BASE="{{ var.value.DATA_ROOT }}"
    EXP="{{ EXP }}"; TRIAL="{{ TRIAL }}"; YEAR="{{ YEAR }}"; MONTH="{{ MONTH }}"
    echo "[cleanup] removing prior outputs for EXP=${EXP}, TRIAL=${TRIAL}, ${YEAR}-${MONTH} in ${BASE}"
    for p in \
      "$BASE/bronze/s3/exp=$EXP/trial=$TRIAL/year=$YEAR/month=$MONTH" \
      "$BASE/silver/s3/exp=$EXP/trial=$TRIAL/year=$YEAR/month=$MONTH" \
      "$BASE/silver_wide/s3/exp=$EXP/trial=$TRIAL/year=$YEAR/month=$MONTH" \
      "$BASE/export/s3/exp=$EXP/trial=$TRIAL/year=$YEAR/month=$MONTH" \
      "$BASE/metrics/s3/exp=$EXP/trial=$TRIAL/year=$YEAR/month=$MONTH" \
      "$BASE/metrics_airflow/s3/exp=$EXP/trial=$TRIAL/year=$YEAR/month=$MONTH"
    do
      echo "  rm -rf $p"; rm -rf "$p" || true
    done
    echo "[cleanup] done"
    '
    """
    cleanup_artifacts = DockerOperator(
        task_id="cleanup_artifacts",
        command=cleanup_cmd,
        **COMMON_DOCKER,
    )
    cleanup_artifacts.doc_md = "Removes previous run artifacts for given EXP/TRIAL/YEAR/MONTH (safe to skip)."

    # ------------------ CHECKS ------------------
    with TaskGroup(group_id="checks") as tg_checks:
        # Paths to Spark jobs inside the container
        bronze_job_path      = "/opt/etl/spark/jobs/bronze_csv_to_parquet.py"
        silver_job_path      = "/opt/etl/spark/jobs/silver_trips_basic.py"
        silver_join_job_path = "/opt/etl/spark/jobs/silver_join.py"
        export_job_path      = "/opt/etl/spark/jobs/export_fact_csv.py"
        dq_silver_job_path   = "/opt/etl/spark/jobs/dq_silver.py"
        dq_join_job_path     = "/opt/etl/spark/jobs/dq_join.py"

        # Short-circuit if a required job is missing
        bronze_job_exists       = ShortCircuitOperator(task_id="bronze_job_exists",       python_callable=short_if_exists, op_kwargs={"path": bronze_job_path})
        silver_job_exists       = ShortCircuitOperator(task_id="silver_job_exists",       python_callable=short_if_exists, op_kwargs={"path": silver_job_path})
        silver_join_job_exists  = ShortCircuitOperator(task_id="silver_join_job_exists",  python_callable=short_if_exists, op_kwargs={"path": silver_join_job_path})
        export_job_exists       = ShortCircuitOperator(task_id="export_job_exists",       python_callable=short_if_exists, op_kwargs={"path": export_job_path})
        dq_silver_job_exists    = ShortCircuitOperator(task_id="dq_silver_job_exists",    python_callable=short_if_exists, op_kwargs={"path": dq_silver_job_path})
        dq_join_job_exists      = ShortCircuitOperator(task_id="dq_join_job_exists",      python_callable=short_if_exists, op_kwargs={"path": dq_join_job_path})

    # ------------------ SPARK ETL ------------------
    with TaskGroup(group_id="spark_etl") as tg_etl:

        bronze  = DockerOperator(task_id="bronze",      command=bronze_cmd,      **COMMON_DOCKER)
        bronze.doc_md = "Reads raw CSV and writes Parquet (bronze). Controlled by `opt`."
        log_bronze = PythonOperator(task_id="log_bronze", python_callable=log_step_metrics, op_kwargs={"step": "bronze"})

        silver  = DockerOperator(task_id="silver_prep", command=silver_cmd,      **COMMON_DOCKER)
        silver.doc_md = "Prepares Silver layer: cleaning / basic transforms."
        log_silver = PythonOperator(task_id="log_silver", python_callable=log_step_metrics, op_kwargs={"step": "silver_prep"})

        # DQ after silver
        dq_silver_job = DockerOperator(task_id="dq_silver_job", command=dq_silver_cmd, **COMMON_DOCKER)
        store_dq_silver = PythonOperator(task_id="store_dq_silver", python_callable=store_dq_file, op_kwargs={"step": "dq_silver"})

        silver_join  = DockerOperator(task_id="silver_join", command=silver_join_cmd, **COMMON_DOCKER)
        silver_join.doc_md = "Joins with reference tables → wide ‘facts’ dataset."
        log_silver_join = PythonOperator(task_id="log_silver_join", python_callable=log_step_metrics, op_kwargs={"step": "silver_join"})

        # DQ after join
        dq_join_job  = DockerOperator(task_id="dq_join_job", command=dq_join_cmd, **COMMON_DOCKER)
        store_dq_join = PythonOperator(task_id="store_dq_join", python_callable=store_dq_file, op_kwargs={"step": "dq_join"})

        export_csv = DockerOperator(task_id="export_csv", command=export_cmd, **COMMON_DOCKER)
        export_csv.doc_md = "Exports to CSV. Supports `single_csv` and `parallel_parts`."
        log_export = PythonOperator(task_id="log_export", python_callable=log_step_metrics, op_kwargs={"step": "export"})

        # Linear chain inside the group
        bronze >> log_bronze \
            >> silver >> log_silver \
            >> dq_silver_job >> store_dq_silver \
            >> silver_join >> log_silver_join \
            >> dq_join_job >> store_dq_join \
            >> export_csv >> log_export

    # ------------------ DWH LOAD ------------------
    with TaskGroup(group_id="dwh_load") as tg_dwh:

        # Top-level gate (on/off)
        dwh_load_gate = ShortCircuitOperator(task_id="dwh_load_gate", python_callable=dwh_load_enabled)
        dwh_load_gate.doc_md = "Gate: enable/disable DWH load steps."

        # Branch gates
        dwh_parallel_gate = ShortCircuitOperator(
            task_id="dwh_parallel_gate",
            python_callable=dwh_parallel_enabled,
            ignore_downstream_trigger_rules=False,
        )
        dwh_single_gate   = ShortCircuitOperator(
            task_id="dwh_single_gate",
            python_callable=dwh_single_enabled,
            ignore_downstream_trigger_rules=False,
        )

        # Load implementations
        load_dwh_single   = PythonOperator(task_id="load_dwh_single",   python_callable=load_fact_single)
        log_dwh_single    = PythonOperator(task_id="log_dwh_single",    python_callable=log_step_metrics, op_kwargs={"step": "dwh_single"})

        load_dwh_parallel = PythonOperator(task_id="load_dwh_parallel", python_callable=load_fact_parallel)
        log_dwh_parallel  = PythonOperator(task_id="log_dwh_parallel",  python_callable=log_step_metrics, op_kwargs={"step": "dwh_parallel"})

        # Merge node: continue if at least one branch succeeded
        dwh_loaded_merge = EmptyOperator(
            task_id="dwh_loaded_merge",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # Wiring inside group
        dwh_load_gate >> Label("dwh_load=true?") >> [dwh_single_gate, dwh_parallel_gate]
        dwh_single_gate   >> load_dwh_single   >> log_dwh_single   >> dwh_loaded_merge
        dwh_parallel_gate >> load_dwh_parallel >> log_dwh_parallel >> dwh_loaded_merge

    # ------------------ DQ POSTLOAD ------------------
    with TaskGroup(group_id="dq_postload") as tg_dq_post:
        dq_dwh_op = PythonOperator(
            task_id="dq_dwh",
            python_callable=dq_dwh_checks,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        dq_dwh_op.doc_md = "DQ in DWH: rowcount mismatch, nulls, negative amounts, etc."
        store_dq_dwh_op = PythonOperator(
            task_id="store_dq_dwh",
            python_callable=store_dq_dwh,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        dq_dwh_op >> store_dq_dwh_op

    # ------------------ Simple Markdown report ------------------
    def render_report(**context):
        """
        Build a small Markdown report for current run from svc.bench_results & svc.dq_results
        and write it to metrics_airflow/.../report.md (handy for defense/demo).
        """
        conf = _get_run_conf(context)
        y = int(conf.get("year", DEFAULT_CONF["year"]))
        m = int(conf.get("month", DEFAULT_CONF["month"]))
        exp = conf.get("experiment_id", DEFAULT_CONF["experiment_id"])
        run_id = context["dag_run"].run_id

        hook = PostgresHook(postgres_conn_id="dwh_postgres")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT step, wall_sec, rows_in, rows_out, bytes_in_mb, bytes_out_mb
                    FROM svc.bench_results
                    WHERE experiment_id=%s AND run_id=%s AND year=%s AND month=%s
                    ORDER BY step
                """, (exp, run_id, y, m))
                bench = cur.fetchall()

                cur.execute("""
                    SELECT step, rule, value_num, threshold, status
                    FROM svc.dq_results
                    WHERE experiment_id=%s AND run_id=%s AND year=%s AND month=%s
                    ORDER BY step, rule
                """, (exp, run_id, y, m))
                dq = cur.fetchall()

        lines = []
        lines.append(f"# Benchmark report\n")
        lines.append(f"- **Experiment:** `{exp}`")
        lines.append(f"- **Run ID:** `{run_id}`")
        lines.append(f"- **Period:** `{y}-{m:02d}`\n")

        lines.append("## Steps (svc.bench_results)\n")
        lines.append("| step | wall_sec | rows_in | rows_out | bytes_in_mb | bytes_out_mb |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for s, w, ri, ro, bi, bo in bench:
            lines.append(f"| {s} | {w or ''} | {ri or ''} | {ro or ''} | {bi or ''} | {bo or ''} |")

        lines.append("\n## DQ (svc.dq_results)\n")
        lines.append("| step | rule | value | threshold | status |")
        lines.append("|---|---|---:|---:|---|")
        for s, r, v, t, st in dq:
            t_str = "" if t is None else f"{t}"
            lines.append(f"| {s} | {r} | {v:.6f} | {t_str} | {st} |")

        report = "\n".join(lines)

        out_dir = _metrics_dir_airflow(conf, y, m)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "report.md")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"[report] written -> {out_path}")

    render_report_task = PythonOperator(
        task_id="render_report",
        python_callable=render_report,
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    render_report_task.doc_md = "Generates a compact Markdown report for the current run."

    # ------------------ Final wiring ------------------
    cleanup_artifacts >> tg_checks >> tg_etl >> tg_dwh >> tg_dq_post >> render_report_task
