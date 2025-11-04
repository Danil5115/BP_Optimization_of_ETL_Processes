from __future__ import annotations

import glob
import json
import os
from decimal import Decimal
from typing import Any, Dict, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from s3_benchmark.config import DATA_ROOT, DEFAULT_CONF


# ====== Paths/files ======
def _exp_base(conf: Dict[str, Any], layer: str) -> str:
    base = os.path.join(DATA_ROOT, layer, "s3", f"exp={conf['experiment_id']}")
    if conf.get("trial") is not None:
        base = os.path.join(base, f"trial={int(conf['trial'])}")
    return base


def _out_dir(conf: Dict[str, Any], layer: str, y: int, m: int) -> str:
    return os.path.join(_exp_base(conf, layer), f"year={y}", f"month={m}")


def _metrics_path(conf: Dict[str, Any], step: str, y: int, m: int) -> str:
    base = os.path.join(DATA_ROOT, "metrics", "s3", f"exp={conf['experiment_id']}")
    if conf.get("trial") is not None:
        base = os.path.join(base, f"trial={int(conf['trial'])}")
    return os.path.join(base, f"year={y}", f"month={m}", f"{step}.json")


def _metrics_dir_airflow(conf: Dict[str, Any], y: int, m: int) -> str:
    base = os.path.join(DATA_ROOT, "metrics_airflow", "s3", f"exp={conf['experiment_id']}")
    if conf.get("trial") is not None:
        base = os.path.join(base, f"trial={int(conf['trial'])}")
    return os.path.join(base, f"year={y}", f"month={m}")


def _sum_bytes(path: str) -> int:
    total = 0
    for f in glob.glob(os.path.join(path, "**", "*"), recursive=True):
        if os.path.isfile(f):
            total += os.path.getsize(f)
    return total


def short_if_exists(path: str, **_) -> bool:
    # Simple helper to check path existence (used for ShortCircuit)
    return os.path.exists(path)


# ====== Run config and helpers ======
def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return False


def _get_run_conf(context) -> Dict[str, Any]:
    # Merge DEFAULT_CONF with dag_run.conf (supports nested default_conf overrides)
    run_conf = dict(DEFAULT_CONF)
    try:
        dag_conf = context["dag_run"].conf or {}
    except Exception:
        dag_conf = {}
    if isinstance(dag_conf.get("default_conf"), dict):
        run_conf.update(dag_conf["default_conf"])
    run_conf.update(dag_conf)
    return run_conf


def _get_ym(context) -> Tuple[int, int]:
    conf = _get_run_conf(context)
    return int(conf.get("year", 2021)), int(conf.get("month", 1))


def _json_default(o):
    # JSON encoder helper for Decimal
    if isinstance(o, Decimal):
        try:
            return int(o)
        except Exception:
            return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


# ====== Step metrics logging into svc.bench_results ======
def log_step_metrics(step: str, **context):
    from typing import Any  # local import to avoid importing typing at module import time

    run_conf: Dict[str, Any] = _get_run_conf(context)
    experiment_id = run_conf.get("experiment_id", DEFAULT_CONF["experiment_id"])
    y = int(run_conf.get("year", DEFAULT_CONF["year"]))
    m = int(run_conf.get("month", DEFAULT_CONF["month"]))
    mode = run_conf.get("mode", "optimized")

    # Metrics file lookup: primary metrics dir, then fallback to Airflow metrics dir
    met_path = _metrics_path(run_conf, step, y, m)
    if not os.path.exists(met_path):
        alt = os.path.join(_metrics_dir_airflow(run_conf, y, m), f"{step}.json")
        if os.path.exists(alt):
            met_path = alt

    wall_sec = rows_in = rows_out = bytes_in = bytes_out = shuffle_read = shuffle_write = None
    spark_conf = run_conf.get("opt", {})
    partitions_in = partitions_out_planned = files_out = files_in = None
    write_sec = merge_sec = None
    tasks_total = stages = None
    disk_spill_mb = memory_spill_mb = None
    gc_time_ms = executor_run_time_ms = None
    skew_factor = None
    wal_bytes_mb = table_bytes_mb = index_bytes_mb = None
    copy_sec_sum = copy_sec_max = analyze_sec = None
    copy_throughput_mib_s = copy_throughput_rows_s = None

    met = {}
    if os.path.exists(met_path):
        try:
            with open(met_path, "r", encoding="utf-8") as f:
                met = json.load(f)
            wall_sec = met.get("wall_sec")
            rows_in = met.get("rows_in")
            rows_out = met.get("rows_out")
            bytes_in = met.get("bytes_in_mb")
            bytes_out = met.get("bytes_out_mb")
            shuffle_read = met.get("shuffle_read_mb")
            shuffle_write = met.get("shuffle_write_mb")
            if isinstance(met.get("spark_conf"), dict):
                spark_conf = met["spark_conf"]

            partitions_in = met.get("partitions_in")
            partitions_out_planned = met.get("partitions_out_planned")
            files_out = met.get("files_out")
            files_in = met.get("files_in")
            write_sec = met.get("write_sec")
            merge_sec = met.get("merge_sec")
            tasks_total = met.get("tasks_total")
            stages = met.get("stages")
            disk_spill_mb = met.get("disk_spill_mb")
            memory_spill_mb = met.get("memory_spill_mb")
            gc_time_ms = met.get("gc_time_ms")
            executor_run_time_ms = met.get("executor_run_time_ms")
            skew_factor = met.get("skew_factor")
            wal_bytes_mb = met.get("wal_bytes_mb")
            table_bytes_mb = met.get("table_bytes_mb")
            index_bytes_mb = met.get("index_bytes_mb")
            copy_sec_sum = met.get("copy_sec_sum")
            copy_sec_max = met.get("copy_sec_max")
            analyze_sec = met.get("analyze_sec")
            copy_throughput_mib_s = met.get("copy_throughput_mib_s")
            copy_throughput_rows_s = met.get("copy_throughput_rows_s")
        except Exception:
            pass

    layer = None
    if step == "bronze":
        layer = "bronze"
    elif step.startswith("silver"):
        layer = "silver"
    elif step.startswith("export"):
        layer = "export"
    if layer:
        out_dir = _out_dir(run_conf, layer, y, m)
        if bytes_out is None and os.path.exists(out_dir):
            bytes_out = _sum_bytes(out_dir) / (1024 * 1024)

    # Keep only unknown metric keys as details payload
    known = {
        "step", "year", "month", "rows_in", "rows_out", "bytes_in_mb", "bytes_out_mb", "wall_sec",
        "shuffle_read_mb", "shuffle_write_mb", "spark_conf",
        "partitions_in", "partitions_out_planned", "files_out", "files_in", "write_sec", "merge_sec",
        "tasks_total", "stages", "disk_spill_mb", "memory_spill_mb", "gc_time_ms", "executor_run_time_ms",
        "skew_factor", "wal_bytes_mb", "table_bytes_mb", "index_bytes_mb",
        "copy_sec_sum", "copy_sec_max", "analyze_sec", "copy_throughput_mib_s", "copy_throughput_rows_s",
    }
    details_dict = {k: v for k, v in (met or {}).items() if k not in known}
    details = json.dumps(details_dict) if details_dict else None

    # Insert a single metrics record into svc.bench_results
    run_id = context["dag_run"].run_id
    hook = PostgresHook(postgres_conn_id="dwh_postgres")
    sql = """
    INSERT INTO svc.bench_results(
      experiment_id, run_id, mode, step,
      year, month, rows_in, rows_out, bytes_in_mb, bytes_out_mb,
      wall_sec, shuffle_read_mb, shuffle_write_mb, spark_conf,
      partitions_in, partitions_out_planned, files_out, files_in,
      write_sec, merge_sec,
      tasks_total, stages, disk_spill_mb, memory_spill_mb, gc_time_ms, executor_run_time_ms, skew_factor,
      wal_bytes_mb, table_bytes_mb, index_bytes_mb,
      copy_sec_sum, copy_sec_max, analyze_sec,
      copy_throughput_mib_s, copy_throughput_rows_s, details
    ) VALUES (
      %s,%s,%s,%s,
      %s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s::jsonb,
      %s,%s,%s,%s,
      %s,%s,
      %s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,
      %s,%s,%s::jsonb
    )
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                experiment_id, run_id, mode, step,
                y, m, rows_in, rows_out, bytes_in, bytes_out,
                wall_sec, shuffle_read, shuffle_write, json.dumps(spark_conf),
                partitions_in, partitions_out_planned, files_out, files_in,
                write_sec, merge_sec,
                tasks_total, stages, disk_spill_mb, memory_spill_mb, gc_time_ms, executor_run_time_ms, skew_factor,
                wal_bytes_mb, table_bytes_mb, index_bytes_mb,
                copy_sec_sum, copy_sec_max, analyze_sec,
                copy_throughput_mib_s, copy_throughput_rows_s, details
            ))
        conn.commit()
