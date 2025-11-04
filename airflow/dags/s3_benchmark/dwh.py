from __future__ import annotations

import glob
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .utils import (
    _get_run_conf,
    _get_ym,
    _metrics_dir_airflow,
    _out_dir,
    _json_default,
)

# ---------------- Partition management helpers ----------------

def _partition_name(y: int, m: int) -> str:
    """Return fully-qualified partition name for (year, month)."""
    return f"dds.fact_trips_y{y}m{m:02d}"


def _expected_bound_text(y: int, m: int) -> str:
    """Return partition bound text as produced by pg_get_expr(relpartbound, oid)."""
    next_y, next_m = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"FOR VALUES FROM ({y}, {m}) TO ({next_y}, {next_m})"


def _regclass_exists(hook: PostgresHook, fqname: str) -> bool:
    """Check if regclass exists for given fully-qualified name."""
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL;", (fqname,))
            return bool(cur.fetchone()[0])


def _find_partition_fqname_by_bounds(hook: PostgresHook, y: int, m: int) -> Optional[str]:
    """
    Find child partition by its relpartbound text, return schema-qualified name.
    We compare exact text produced by pg_get_expr(relpartbound, oid).
    """
    expected = _expected_bound_text(y, m)
    sql = """
        SELECT n.nspname || '.' || c.relname
        FROM pg_inherits i
        JOIN pg_class c     ON c.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.inhparent = 'dds.fact_trips'::regclass
          AND pg_get_expr(c.relpartbound, c.oid) = %s
        LIMIT 1;
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (expected,))
            row = cur.fetchone()
            return row[0] if row else None


def _ensure_month_partition(hook: PostgresHook, y: int, m: int) -> str:
    """
    Ensure a monthly partition for (y, m) exists. Return its schema-qualified name.

    Algorithm:
      1) Try to find it by exact bound text.
      2) If not found, create it and its indexes (index names without schema!).
      3) Re-check by bounds; if still not found, fall back to expected name.
      4) If nothing found, raise a clear error.
    """
    # 1) already present by bounds?
    fq = _find_partition_fqname_by_bounds(hook, y, m)
    if fq:
        return fq

    # 2) create (inline numeric literals in DDL to avoid param binding issues)
    next_y, next_m = (y + 1, 1) if m == 12 else (y, m + 1)
    rel = _partition_name(y, m)   # e.g. dds.fact_trips_y2021m01
    tbl = rel.split(".")[-1]      # table name without schema for index names

    create_sql = (
        f"CREATE TABLE {rel} PARTITION OF dds.fact_trips "
        f"FOR VALUES FROM ({y}, {m}) TO ({next_y}, {next_m});"
    )
    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                # Index names must not be schema-qualified
                cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_ym         ON {rel} (year, month);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_src        ON {rel} (load_src);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_pick_date  ON {rel} (pickup_date_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_drop_date  ON {rel} (dropoff_date_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_pickup_ts  ON {rel} (pickup_ts);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_dropoff_ts ON {rel} (dropoff_ts);")
            conn.commit()
    except Exception:
        # Swallow create-time races/duplicates: we will verify existence below.
        pass

    # 3) check again by bounds
    fq = _find_partition_fqname_by_bounds(hook, y, m)
    if fq:
        return fq

    # fall back to expected name if the relation exists
    if _regclass_exists(hook, rel):
        return rel

    # 4) final failure
    raise RuntimeError(
        f"Partition for ({y},{m}) was not found and could not be created. "
        f"Tried name: {rel}; bounds: {_expected_bound_text(y, m)}"
    )


def _truncate_partition(hook: PostgresHook, y: int, m: int) -> None:
    """TRUNCATE the partition that covers (y, m)."""
    fq = _find_partition_fqname_by_bounds(hook, y, m)
    if not fq:
        fq = _ensure_month_partition(hook, y, m)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {fq};")
        conn.commit()


def _sizes_for_partition(hook: PostgresHook, y: int, m: int) -> Tuple[Optional[float], Optional[float]]:
    """Return (table_size_mb, index_size_mb) for a concrete partition or (None, None)."""
    fq = _find_partition_fqname_by_bounds(hook, y, m)
    if not fq:
        rel = _partition_name(y, m)
        if _regclass_exists(hook, rel):
            fq = rel
        else:
            return None, None

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_table_size(to_regclass(%s)), pg_indexes_size(to_regclass(%s));",
                (fq, fq),
            )
            t_sz, i_sz = cur.fetchone()
            table_bytes_mb = round((t_sz or 0) / (1024 * 1024), 3)
            index_bytes_mb = round((i_sz or 0) / (1024 * 1024), 3)
            return table_bytes_mb, index_bytes_mb


# ---------------- DWH load (single-file COPY) ----------------

def load_fact_single(**context):
    """
    Load single exported CSV file into dds.fact_trips (partitioned by year, month).
    Ensures partition exists and is truncated before COPY. Collects WAL/size metrics.
    """
    t0 = time.time()
    conf = _get_run_conf(context)
    y, m = _get_ym(context)

    export_dir = _out_dir(conf, "export", y, m)
    files = sorted(glob.glob(os.path.join(export_dir, "*.csv")))
    if not files:
        raise FileNotFoundError(f"No CSV to load: {export_dir}")
    csv_path = files[0]

    hook = PostgresHook(postgres_conn_id="dwh_postgres")

    copy_sql = """
    COPY dds.fact_trips (
      vendor_id, pickup_ts, dropoff_ts, pickup_date_id, dropoff_date_id,
      passenger_count, trip_distance,
      ratecode_id, store_and_fwd_flag, pu_zone_id, do_zone_id,
      payment_type, fare_amount, extra, mta_tax, tip_amount,
      tolls_amount, improvement_surcharge, total_amount,
      congestion_surcharge, year, month, load_src, row_hash
    ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    """

    # Ensure/clean target partition
    _ensure_month_partition(hook, y, m)
    _truncate_partition(hook, y, m)

    wal_before = None
    wal_after = None
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT pg_current_wal_lsn()::text;")
                wal_before = cur.fetchone()[0]
            except Exception:
                wal_before = None

            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)

            try:
                cur.execute("SELECT pg_current_wal_lsn()::text;")
                wal_after = cur.fetchone()[0]
            except Exception:
                wal_after = None
        conn.commit()

    wall = time.time() - t0

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM dds.fact_trips WHERE year=%s AND month=%s;",
                (y, m),
            )
            rows_loaded = int(cur.fetchone()[0])

    table_bytes_mb, index_bytes_mb = _sizes_for_partition(hook, y, m)

    wal_bytes_mb = None
    if wal_before and wal_after:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT pg_wal_lsn_diff(%s, %s);", (wal_after, wal_before))
                    wal_bytes = cur.fetchone()[0]
                    try:
                        wal_bytes = int(wal_bytes)
                    except Exception:
                        wal_bytes = float(wal_bytes)
                    wal_bytes_mb = round(float(wal_bytes) / (1024 * 1024), 3)
                except Exception:
                    wal_bytes_mb = None

    bytes_in_mb = round(os.path.getsize(csv_path) / (1024 * 1024), 3)
    thr_mib_s = round((bytes_in_mb / wall), 6) if wall and bytes_in_mb else None
    thr_rows_s = round((rows_loaded / wall), 6) if wall and rows_loaded else None

    met_dir = _metrics_dir_airflow(conf, y, m)
    os.makedirs(met_dir, exist_ok=True)
    metrics = {
        "step": "dwh_single",
        "year": y,
        "month": m,
        "rows_in": rows_loaded,
        "rows_out": rows_loaded,
        "bytes_in_mb": bytes_in_mb,
        "bytes_out_mb": None,
        "wall_sec": round(wall, 3),
        "spark_conf": {"loader": "single_copy"},
        "wal_bytes_mb": wal_bytes_mb,
        "table_bytes_mb": table_bytes_mb,
        "index_bytes_mb": index_bytes_mb,
        "copy_throughput_mib_s": thr_mib_s,
        "copy_throughput_rows_s": thr_rows_s,
    }
    with open(os.path.join(met_dir, "dwh_single.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2, default=_json_default)


# ---------------- DWH load (parallel parts -> staging -> fact) ----------------

def _list_saved_parts(conf: Dict[str, Any], y: int, m: int) -> List[str]:
    """List part CSV files produced by the exporter."""
    export_dir = _out_dir(conf, "export", y, m)
    parts_dir = os.path.join(export_dir, "parts")
    return sorted(glob.glob(os.path.join(parts_dir, "*.csv")))


def dwh_parallel_enabled(**context) -> bool:
    """Branch helper: return True iff parallel DWH load is enabled in run config."""
    conf = _get_run_conf(context)
    return bool(conf.get("dwh_parallel", {}).get("enabled", False))


def dwh_single_enabled(**context) -> bool:
    """Branch helper: run single-copy path when parallel is disabled."""
    return not dwh_parallel_enabled(**context)


def load_fact_parallel(**context):
    """
    Load multiple part CSV files via a staging table, then INSERT into fact table
    (routing to the right partition by (year, month)). Optionally ANALYZE.
    """
    t0 = time.time()
    conf = _get_run_conf(context)
    y, m = _get_ym(context)
    dwhp = conf.get("dwh_parallel", {}) or {}
    workers = int(dwhp.get("workers", 4))
    staging_schema = dwhp.get("staging_schema", "stg")
    staging_unlogged = bool(dwhp.get("staging_unlogged", True))
    keep_staging = bool(dwhp.get("keep_staging", False))
    sync_off = bool(dwhp.get("sync_commit_off", True))
    analyze_after = bool(dwhp.get("analyze_after", True))

    part_files = _list_saved_parts(conf, y, m)
    if not part_files:
        raise FileNotFoundError("No part CSV found. Ensure export_strategy='parallel_parts'.")

    hook = PostgresHook(postgres_conn_id="dwh_postgres")
    table_main = "dds.fact_trips"
    table_stg = f"{staging_schema}.fact_trips_y{y}m{m:02d}"

    # Ensure/clean target partition
    fq = _ensure_month_partition(hook, y, m)
    _truncate_partition(hook, y, m)

    # Prepare staging
    with hook.get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};")
            unlogged = "UNLOGGED" if staging_unlogged else ""
            cur.execute(f"DROP TABLE IF EXISTS {table_stg};")
            cur.execute(f"CREATE {unlogged} TABLE {table_stg} (LIKE {table_main} INCLUDING DEFAULTS);")
            cur.execute(f"TRUNCATE {table_stg};")

    copy_sql = f"""
    COPY {table_stg} (
      vendor_id, pickup_ts, dropoff_ts, pickup_date_id, dropoff_date_id,
      passenger_count, trip_distance,
      ratecode_id, store_and_fwd_flag, pu_zone_id, do_zone_id,
      payment_type, fare_amount, extra, mta_tax, tip_amount,
      tolls_amount, improvement_surcharge, total_amount,
      congestion_surcharge, year, month, load_src, row_hash
    ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    """

    def _copy_one(path: str) -> float:
        t1 = time.time()
        with hook.get_conn() as c2:
            with c2.cursor() as cur2:
                if sync_off:
                    cur2.execute("SET LOCAL synchronous_commit = off;")
                with open(path, "r", encoding="utf-8") as f:
                    cur2.copy_expert(copy_sql, f)
                c2.commit()
        return time.time() - t1

    wal_before = None
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT pg_current_wal_lsn()::text;")
                wal_before = cur.fetchone()[0]
            except Exception:
                wal_before = None

    copy_times = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut2path = {ex.submit(_copy_one, p): p for p in part_files}
        for fut in as_completed(fut2path):
            copy_times.append(fut.result())

    # Move from staging to fact (partition routing happens automatically)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute(f"INSERT INTO dds.fact_trips SELECT * FROM {table_stg};")
            cur.execute("COMMIT;")

    analyze_sec = None
    if analyze_after:
        tA = time.time()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"ANALYZE {fq};")
                conn.commit()
        analyze_sec = time.time() - tA

    if not keep_staging:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_stg};")
                conn.commit()

    wal_after = None
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT pg_current_wal_lsn()::text;")
                wal_after = cur.fetchone()[0]
            except Exception:
                wal_after = None

    table_bytes_mb, index_bytes_mb = _sizes_for_partition(hook, y, m)

    wal_bytes_mb = None
    if wal_before and wal_after:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT pg_wal_lsn_diff(%s, %s);", (wal_after, wal_before))
                    wal_bytes = cur.fetchone()[0]
                    try:
                        wal_bytes = int(wal_bytes)
                    except Exception:
                        wal_bytes = float(wal_bytes)
                    wal_bytes_mb = round(float(wal_bytes) / (1024 * 1024), 3)
                except Exception:
                    wal_bytes_mb = None

    wall = time.time() - t0
    bytes_in = sum(os.path.getsize(p) for p in part_files) / (1024 * 1024)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM dds.fact_trips WHERE year=%s AND month=%s;",
                (y, m),
            )
            rows_loaded = int(cur.fetchone()[0])

    thr_mib_s = round((bytes_in / wall), 6) if wall and bytes_in else None
    thr_rows_s = round((rows_loaded / wall), 6) if wall and rows_loaded else None

    met_dir = _metrics_dir_airflow(conf, y, m)
    os.makedirs(met_dir, exist_ok=True)
    met = {
        "step": "dwh_parallel",
        "year": y,
        "month": m,
        "rows_in": rows_loaded,
        "rows_out": rows_loaded,
        "bytes_in_mb": round(bytes_in, 3),
        "bytes_out_mb": None,
        "wall_sec": round(wall, 3),
        "spark_conf": {
            "dwh_parallel": {
                "workers": workers,
                "staging_unlogged": staging_unlogged,
                "sync_commit_off": sync_off,
                "analyze_after": analyze_after,
            }
        },
        "files_in": len(part_files),
        "copy_times_sec": copy_times,
        "copy_sec_sum": round(sum(copy_times), 3),
        "copy_sec_max": round(max(copy_times) if copy_times else 0.0, 3),
        "analyze_sec": round(analyze_sec, 3) if analyze_sec is not None else None,
        "wal_bytes_mb": wal_bytes_mb,
        "table_bytes_mb": table_bytes_mb,
        "index_bytes_mb": index_bytes_mb,
        "copy_throughput_mib_s": thr_mib_s,
        "copy_throughput_rows_s": thr_rows_s,
    }
    with open(os.path.join(met_dir, "dwh_parallel.json"), "w", encoding="utf-8") as f:
        json.dump(met, f, ensure_ascii=False, indent=2, default=_json_default)
