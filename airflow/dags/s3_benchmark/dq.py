# DQ utilities: write DQ results, apply thresholds, gate failures, and run DWH checks.

from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from s3_benchmark.utils import _get_run_conf, _get_ym, _metrics_path, _metrics_dir_airflow


def _dq_write_rows(step: str, rows: List[tuple]):
    # Write DQ result rows into svc.dq_results (Postgres)
    hook = PostgresHook(postgres_conn_id="dwh_postgres")
    sql = """
    INSERT INTO svc.dq_results (
        experiment_id, run_id, step, year, month,
        rule, value_num, threshold, status, details
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(sql, r)
        conn.commit()


def _dq_status(val: float, thr: float) -> str:
    # Return OK if value <= threshold (or threshold is None), otherwise FAIL
    if thr is None:
        return "OK"
    return "OK" if val <= thr else "FAIL"


def store_dq_file(step: str, **context):
    # Load metrics JSON for the step, compare with thresholds, write to DB, and optionally gate failures
    conf = _get_run_conf(context)
    y, m = _get_ym(context)
    run_id = context["dag_run"].run_id
    exp = conf["experiment_id"]
    dq_conf = (conf.get("dq") or {})
    thresholds = (dq_conf.get("silver") if step == "dq_silver"
                  else dq_conf.get("join") if step == "dq_join"
                  else dq_conf.get("dwh", {}))

    # Resolve metrics path with fallback to Airflow metrics dir
    met_path = _metrics_path(conf, step, y, m)
    if not os.path.exists(met_path):
        alt = os.path.join(_metrics_dir_airflow(conf, y, m), f"{step}.json")
        met_path = alt if os.path.exists(alt) else met_path
    if not os.path.exists(met_path):
        raise FileNotFoundError(f"DQ metrics file not found for {step}: {met_path}")

    with open(met_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    mp = data.get("metrics_pct") or {}
    rows: List[tuple] = []
    for k, v in mp.items():
        thr_key = f"{k}_max"
        thr = thresholds.get(thr_key)
        status = _dq_status(float(v), float(thr)) if thr is not None else "OK"
        rows.append((
            exp, run_id, step, y, m, k,
            float(v),
            float(thr) if thr is not None else None,
            status,
            json.dumps({"source": "file", "path": met_path})
        ))
    _dq_write_rows(step, rows)

    # Gate: if this step is configured to fail the run, raise on any FAIL
    gate_steps = set((dq_conf.get("gate_fail_on") or []))
    if step in gate_steps:
        if any(r[8] == "FAIL" for r in rows):
            raise RuntimeError(
                f"DQ gate failed at {step}: "
                f"{[(r[5], r[6], r[7]) for r in rows if r[8]=='FAIL']}"
            )


def dq_dwh_checks(**context):
    # Compute DWH-level DQ metrics and write them to a metrics JSON file
    conf = _get_run_conf(context)
    y, m = _get_ym(context)
    hook = PostgresHook(postgres_conn_id="dwh_postgres")

    # Read export metrics (rows_out/rows_in) to compare with DWH row count
    export_json = _metrics_path(conf, "export", y, m)
    rows_export = None
    if os.path.exists(export_json):
        try:
            with open(export_json, "r", encoding="utf-8") as f:
                ej = json.load(f)
            rows_export = ej.get("rows_out") or ej.get("rows_in")
        except Exception:
            rows_export = None

    # Aggregate DWH metrics for the month
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dds.fact_trips WHERE year=%s AND month=%s;", (y, m))
            rows_fact = int(cur.fetchone()[0])

            cur.execute("""
              SELECT
                 100.0*AVG((pu_zone_id IS NULL OR do_zone_id IS NULL)::int),
                 100.0*AVG((payment_type IS NULL)::int),
                 100.0*AVG((ratecode_id  IS NULL)::int),
                 100.0*AVG((vendor_id    IS NULL)::int),
                 100.0*AVG(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END)
              FROM dds.fact_trips
              WHERE year=%s AND month=%s;
            """, (y, m))
            (null_zone_pct, null_pay_pct, null_rate_pct, null_vendor_pct, negative_total_pct) = \
                [float(x or 0.0) for x in cur.fetchone()]

    results = {}
    if rows_export and rows_export > 0:
        mismatch_pct = abs(rows_export - rows_fact) / rows_export * 100.0
        results["rowcount_mismatch_pct"] = mismatch_pct
    else:
        results["rowcount_mismatch_pct"] = 0.0
    results.update({
        "null_zone_id_pct":      null_zone_pct,
        "null_payment_type_pct": null_pay_pct,
        "null_ratecode_id_pct":  null_rate_pct,
        "null_vendor_id_pct":    null_vendor_pct,
        "negative_total_pct":    negative_total_pct
    })

    out = {
        "step": "dq_dwh",
        "year": y, "month": m,
        "metrics_pct": {k: round(v, 6) for k, v in results.items()},
        "rows_fact": rows_fact,
        "rows_export": rows_export
    }

    # Persist dq_dwh.json under the Airflow metrics directory
    met_dir = _metrics_dir_airflow(conf, y, m)
    os.makedirs(met_dir, exist_ok=True)
    met_path = os.path.join(met_dir, "dq_dwh.json")
    with open(met_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


def store_dq_dwh(**context):
    # Convenience wrapper to store DWH DQ metrics into DB
    store_dq_file("dq_dwh", **context)
