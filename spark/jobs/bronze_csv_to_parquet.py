from __future__ import annotations

import argparse
import glob
import io
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession


# ===== Utilities =============================================================

MB = 1024 * 1024


def _eventlog_stats(eventlog_dir: str, app_id: str) -> Dict[str, Any]:
    """
    Parse Spark eventlog (JSON lines) and extract light-weight runtime stats.

    Returns keys (0 if missing):
      - shuffle_read_mb / shuffle_write_mb
      - tasks_total / stages
      - disk_spill_mb / memory_spill_mb
      - gc_time_ms / executor_run_time_ms
      - skew_factor  (max(task_run_time) / mean(task_run_time), if available)
    """
    # Typical eventlog filename is "<appId>" or "<appId>-<timestamp>".
    base = Path(eventlog_dir)
    path = base / app_id
    if not path.exists():
        matches = list(base.glob(f"{app_id}*"))
        path = matches[0] if matches else None  # type: ignore[assignment]

    if not path or not Path(path).exists():  # nothing to parse
        return {
            "shuffle_read_mb": 0,
            "shuffle_write_mb": 0,
            "tasks_total": 0,
            "stages": 0,
            "disk_spill_mb": 0,
            "memory_spill_mb": 0,
            "gc_time_ms": 0,
            "executor_run_time_ms": 0,
            "skew_factor": None,
        }

    read_b = write_b = mem_spill = disk_spill = gc_ms = run_ms = 0
    tasks = 0
    run_times: List[int] = []
    stages_seen = set()

    # Eventlog is JSONL; we tolerate occasional broken lines.
    with io.open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                ev = json.loads(line)
            except Exception:
                continue

            et = ev.get("Event")
            if et == "SparkListenerTaskEnd":
                tasks += 1
                tm = ev.get("Task Metrics") or {}

                srm = tm.get("Shuffle Read Metrics") or {}
                # Some Spark versions expose "Total Bytes Read", for others we sum Remote+Local.
                total_read = srm.get("Total Bytes Read")
                if total_read is None:
                    total_read = (srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0))
                read_b += int(total_read or 0)

                swm = tm.get("Shuffle Write Metrics") or {}
                # Fallback: "Bytes Written" (older struct) if "Shuffle Bytes Written" is missing.
                wb = swm.get("Shuffle Bytes Written")
                if wb is None:
                    wb = swm.get("Bytes Written", 0)
                write_b += int(wb or 0)

                mem_spill += int(tm.get("Memory Bytes Spilled", 0))
                disk_spill += int(tm.get("Disk Bytes Spilled", 0))
                gc_ms += int(tm.get("JVM GC Time", 0))

                r = int(tm.get("Executor Run Time", 0))
                run_ms += r
                if r > 0:
                    run_times.append(r)

            elif et == "SparkListenerStageCompleted":
                si = ev.get("Stage Info") or {}
                sid = si.get("Stage ID")
                if sid is not None:
                    stages_seen.add(sid)

    skew = None
    if run_times:
        mx = max(run_times)
        mean = sum(run_times) / len(run_times)
        if mean > 0:
            skew = round(mx / mean, 6)

    return {
        "shuffle_read_mb": round(read_b / MB, 3),
        "shuffle_write_mb": round(write_b / MB, 3),
        "tasks_total": tasks,
        "stages": len(stages_seen),
        "disk_spill_mb": round(disk_spill / MB, 3),
        "memory_spill_mb": round(mem_spill / MB, 3),
        "gc_time_ms": gc_ms,
        "executor_run_time_ms": run_ms,
        "skew_factor": skew,
    }


def pick_input_glob(
    data_root: str, dataset: str, year: int, month: int, explicit: Optional[str] = None
) -> str:
    """
    Resolve an input CSV glob pattern.

    Search order:
      1) explicit pattern (if provided)
      2) landing/<dataset>/year=<Y>/month=<M>/*.csv
      3) landing/<dataset>/<YYYY>/<MM>/*.csv

    Raises FileNotFoundError if nothing matches.
    """
    candidates = []
    if explicit:
        candidates.append(explicit)

    # Support both partitioned and plain year/month directory layouts.
    candidates.append(os.path.join(data_root, "landing", dataset, f"year={year}", f"month={month}", "*.csv"))
    candidates.append(os.path.join(data_root, "landing", dataset, f"{year}", f"{month:02d}", "*.csv"))

    for pat in candidates:
        if glob.glob(pat):
            return pat

    msg = [
        "Input CSV files were not found. Tried:",
        *[f" - {p}" for p in candidates],
        "Check your landing loader output.",
    ]
    raise FileNotFoundError("\n".join(msg))


def _dir_size_bytes(path: Path) -> int:
    """Compute total size of all files under a directory (best-effort)."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def _files_size_bytes(pattern: str) -> int:
    """Compute total size of files matching a glob pattern (best-effort)."""
    total = 0
    for fp in glob.glob(pattern):
        try:
            total += os.path.getsize(fp)
        except OSError:
            pass
    return total


def _bronze_out_dir(args: argparse.Namespace) -> Path:
    """
    Build the bronze output directory:
      bronze/s3/exp=<exp>/[trial=<N>/]year=<Y>/month=<M>
    """
    base = Path(args.data_root) / "bronze" / "s3" / f"exp={args.experiment_id}"
    if args.trial is not None:
        base = base / f"trial={int(args.trial)}"
    return base / f"year={args.year}" / f"month={args.month}"


# ===== Main job =============================================================

def main() -> None:
    # --- CLI args (kept 100% backward compatible) ---------------------------
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    ap.add_argument("--data-root", default="/opt/etl/data")
    ap.add_argument("--input", dest="input_glob", default=None)

    ap.add_argument("--parquet-codec", default="snappy")
    ap.add_argument("--shuffle-partitions", type=int, default=64)
    ap.add_argument("--max-partition-bytes", type=int, default=128 * 1024 * 1024)
    ap.add_argument("--aqe", action="store_true")
    ap.add_argument("--broadcast-small", action="store_true")
    ap.add_argument("--target-files-per-month", type=int, default=1)

    ap.add_argument("--experiment-id", default=None)
    ap.add_argument("--trial", type=int, default=None)
    args = ap.parse_args()

    # --- Resolve input & prepare eventlog -----------------------------------
    input_glob = pick_input_glob(args.data_root, args.dataset, args.year, args.month, args.input_glob)
    eventlog_dir = os.path.join(args.data_root, "_spark_eventlog")
    os.makedirs(eventlog_dir, exist_ok=True)

    # --- Spark session (only the knobs we actually use) ---------------------
    builder = (
        SparkSession.builder.appName("bronze_csv_to_parquet")
        .config("spark.master", "local[*]")  # local mode is sufficient for the benchmark container
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .config("spark.sql.files.maxPartitionBytes", args.max_partition_bytes)
        .config("spark.sql.adaptive.enabled", "true" if args.aqe else "false")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{eventlog_dir}")
        .config("spark.eventLog.compress", "false")
    )
    # Optional: allow broadcast joins for small refs
    if args.broadcast_small:
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", 50 * MB)

    spark = builder.getOrCreate()
    app_id = spark.sparkContext.applicationId
    spark.sparkContext.setLogLevel("WARN")

    # --- Read CSV, plan output parallelism ----------------------------------
    t0 = time.perf_counter()
    df = spark.read.option("header", True).option("multiLine", False).csv(input_glob)

    # We record input partitions for observability (no shuffle yet).
    partitions_in = df.rdd.getNumPartitions()
    try:
        rows_in = df.count()  # simple “ingest” metric; safe for bronze step
    except Exception:
        rows_in = None

    out_dir = _bronze_out_dir(args)
    out_dir.mkdir(parents=True, exist_ok=True)

    num_out = max(1, int(args.target_files_per_month))
    # Single output file -> coalesce(1); multiple files -> repartition(N)
    writer_df = df.repartition(num_out) if num_out > 1 else df.coalesce(1)
    partitions_out_planned = writer_df.rdd.getNumPartitions()

    (
        writer_df.write
        .mode("overwrite")
        .option("compression", args.parquet_codec)
        .parquet(str(out_dir))
    )

    rows_out = rows_in
    elapsed = time.perf_counter() - t0

    # --- I/O sizes (rough, but stable and useful for comparisons) -----------
    bytes_in_mb = _files_size_bytes(input_glob) / MB
    bytes_out_mb = _dir_size_bytes(out_dir) / MB

    # --- Stop Spark & collect eventlog stats --------------------------------
    spark.stop()
    ev = _eventlog_stats(eventlog_dir, app_id)
    files_out = len(list(out_dir.glob("part-*.parquet")))

    # --- Persist metrics (if experiment_id provided) ------------------------
    if args.experiment_id:
        met_base = Path(args.data_root) / "metrics" / "s3" / f"exp={args.experiment_id}"
        if args.trial is not None:
            met_base = met_base / f"trial={int(args.trial)}"
        met_path = met_base / f"year={args.year}" / f"month={args.month}" / "bronze.json"
        met_path.parent.mkdir(parents=True, exist_ok=True)

        payload: Dict[str, Any] = {
            "step": "bronze",
            "dataset": args.dataset,
            "year": args.year,
            "month": args.month,
            "wall_sec": elapsed,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "bytes_in_mb": round(bytes_in_mb, 3),
            "bytes_out_mb": round(bytes_out_mb, 3),
            "partitions_in": partitions_in,
            "partitions_out_planned": partitions_out_planned,
            "files_out": files_out,
            "spark_conf": {
                "parquet_codec": args.parquet_codec,
                "shuffle_partitions": args.shuffle_partitions,
                "max_partition_bytes": args.max_partition_bytes,
                "aqe": bool(args.aqe),
                "broadcast_small": bool(args.broadcast_small),
                "target_files_per_month": num_out,
            },
            "app_id": app_id,
            "eventlog_dir": eventlog_dir,
            # From eventlog: shuffle, spills, gc, run_time, skew
            **ev,
        }
        with open(met_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"[bronze] done in {elapsed:.2f}s | in_parts={partitions_in} -> out_files={files_out} | "
        f"shuffle R/W: {ev.get('shuffle_read_mb', 0)}/{ev.get('shuffle_write_mb', 0)} MiB"
    )


if __name__ == "__main__":
    main()
