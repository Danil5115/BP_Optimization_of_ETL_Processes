from __future__ import annotations

import os
import csv
import datetime as dt
from typing import Dict, Tuple

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

# ===== Parameters and paths =====
DATA_ROOT: str   = Variable.get("DATA_ROOT", "/opt/etl/data")
SPARK_IMAGE: str = Variable.get("SPARK_IMAGE", "my-spark:latest")

REF_DIR = os.path.join(DATA_ROOT, "ref")

def parq_path(name: str) -> str:
    return os.path.join(REF_DIR, name, f"{name}.parquet")

def csv_path(name: str) -> str:
    return os.path.join(REF_DIR, f"{name}.csv")

# --- Docker mounts (volume names from docker-compose) ---
etl_data_mount   = Mount(source="etl_data",   target="/opt/etl/data",       type="volume")
spark_jobs_mount = Mount(source="spark_jobs", target="/opt/etl/spark/jobs", type="volume")

COMPOSE_NETWORK = None  # e.g. "bp_optimization_of_etl_processes_default"

COMMON_DOCKER: Dict = dict(
    image=SPARK_IMAGE,
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode=COMPOSE_NETWORK,
    mounts=[etl_data_mount, spark_jobs_mount],
    mount_tmp_dir=False,
    tty=False,
)

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"  # for apache/spark:3.5.x

# ===== Reference specs (single source of truth) =====
# key -> (csv_name, parquet_name, dwh_table, columns_sql)
REF_SPECS: Dict[str, Tuple[str, str, str, str]] = {
    "vendor":      ("vendor",      "vendor",      "dds.dim_vendor",        "(vendor_id, vendor_name)"),
    "ratecode":    ("ratecode",    "ratecode",    "dds.dim_ratecodes",     "(ratecode_id, description)"),
    "payment_type":("payment_type","payment_type","dds.dim_payment_type",  "(payment_type_id, description)"),
    "store_flag":  ("store_flag",  "store_flag",  "dds.dim_store_flag",    "(store_flag, description)"),
    "zones":       ("zones",       "zones",       "dds.dim_zones",         "(zone_id, borough, zone_name, service_zone)"),
    "dim_date":    ("dim_date",    "dim_date",    "dds.dim_date",
                    '(date_id, "date", year, quarter, month, day, iso_week, iso_dow, is_weekend, is_month_start, is_month_end)'),
}

# ===== Helper Python functions =====
def generate_dim_date_csv(**_):
    start = Variable.get("DATE_START", "2020-01-01")
    end   = Variable.get("DATE_END",   "2021-12-31")
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)

    path = csv_path("dim_date")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "date_id","date","year","quarter","month","day",
            "iso_week","iso_dow","is_weekend","is_month_start","is_month_end"
        ])
        cur = d0
        while cur <= d1:
            date_id = int(cur.strftime("%Y%m%d"))
            year, month, day = cur.year, cur.month, cur.day
            quarter = (month - 1)//3 + 1
            iso_week = cur.isocalendar().week
            iso_dow = cur.isoweekday()
            is_weekend = iso_dow >= 6
            is_month_start = day == 1
            # month-end: tomorrow belongs to a different month
            is_month_end = (cur + dt.timedelta(days=1)).month != month
            w.writerow([
                date_id, cur.isoformat(), year, quarter, month, day,
                iso_week, iso_dow, is_weekend, is_month_start, is_month_end
            ])
            cur += dt.timedelta(days=1)

def normalize_zones_csv(**_):
    """Normalize taxi_zone_lookup.csv to columns: zone_id, borough, zone_name, service_zone."""
    src = os.path.join(REF_DIR, "taxi_zone_lookup.csv")
    dst = csv_path("zones")
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    with open(src, "r", encoding="utf-8-sig") as f_in, open(dst, "w", newline="", encoding="utf-8") as f_out:
        r = csv.DictReader(f_in)
        w = csv.writer(f_out)
        w.writerow(["zone_id","borough","zone_name","service_zone"])
        for row in r:
            w.writerow([row["LocationID"], row["Borough"], row["Zone"], row["service_zone"]])

def load_csv_via_copy(table: str, path_csv: str, columns_sql: str):
    """
    Initial dimension load.
    TRUNCATE ... CASCADE — to avoid FK issues during a full reload.
    """
    hook = PostgresHook(postgres_conn_id="dwh_postgres")
    sql_clear = f"TRUNCATE TABLE {table} CASCADE;"
    copy_sql  = f"COPY {table} {columns_sql} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');"
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_clear)
            with open(path_csv, "r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)
        conn.commit()

def make_loader(table: str, csv_name: str, columns_sql: str):
    def _load(**_):
        load_csv_via_copy(table, csv_path(csv_name), columns_sql)
    return _load

def make_ref_to_parquet_command(csv_name: str, parquet_name: str) -> str:
    return (
        f"{SPARK_SUBMIT} /opt/etl/spark/jobs/ref_to_parquet.py "
        f"--dataset {parquet_name} --input {csv_path(csv_name)} --output {parq_path(parquet_name)}"
    )

# ===== DAG =====
with DAG(
    dag_id="init_dims",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["init", "dimensions"],
    default_args={"owner": "de", "retries": 1},
) as dag:

    # Pre-steps: prepare source CSV files
    gen_dim_date_csv = PythonOperator(
        task_id="generate_dim_date_csv",
        python_callable=generate_dim_date_csv,
    )
    norm_zones = PythonOperator(
        task_id="normalize_zones_csv",
        python_callable=normalize_zones_csv,
    )

    # Group: CSV -> Parquet conversion
    with TaskGroup(group_id="csv_to_parquet") as g_parquet:
        parquet_tasks = {}
        for key, (csv_nm, parq_nm, _table, _cols) in REF_SPECS.items():
            task = DockerOperator(
                task_id=f"{key}_csv_to_parquet",
                command=make_ref_to_parquet_command(csv_nm, parq_nm),
                **COMMON_DOCKER,
            )
            parquet_tasks[key] = task

        # dependencies for preparatory steps
        parquet_tasks["dim_date"].set_upstream(gen_dim_date_csv)
        parquet_tasks["zones"].set_upstream(norm_zones)

    # Group: load into DWH
    with TaskGroup(group_id="load_dwh") as g_load:
        for key, (csv_nm, _parq_nm, table, cols) in REF_SPECS.items():
            PythonOperator(
                task_id=f"load_{key}",
                python_callable=make_loader(table, csv_nm, cols),
            )

    # CSV -> Parquet -> DWH
    g_parquet >> g_load
