from __future__ import annotations

from airflow.models import Variable
from docker.types import Mount

# ====== Константы и окружение ======
DATA_ROOT = Variable.get("DATA_ROOT", "/opt/etl/data")
SPARK_IMAGE = Variable.get("SPARK_IMAGE", "my-spark:latest")
COMPOSE_NETWORK = None  # if a docker-compose network is needed — specify its name

# docker mounts
etl_data_mount   = Mount(source="etl_data",   target="/opt/etl/data",       type="volume")
spark_jobs_mount = Mount(source="spark_jobs", target="/opt/etl/spark/jobs", type="volume")

COMMON_DOCKER = dict(
    image=SPARK_IMAGE,
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode=COMPOSE_NETWORK,
    mounts=[etl_data_mount, spark_jobs_mount],
    mount_tmp_dir=False,
    tty=False,
)

# ====== DEFAULTS ======
DEFAULT_CONF = {
    "dataset": "m6nq-qud6",
    "year": 2021,
    "month": 1,
    "experiment_id": "opt_default",
    "trial": 1,
    "cleanup": False,

    # enables/disables loading into DWH + subsequent DQ in DWH
    "dwh_load": False,

    # Export:
    #  - "single_csv"
    #  - "demo_parallel_merge"
    #  - "parallel_parts"
    "export_strategy": "single_csv",

    # Spark optimizations
    "opt": {
        "aqe": True,
        "parquet_codec": "snappy",
        "shuffle_partitions": 64,
        "max_partition_bytes": 128 * 1024 * 1024,
        "partition_by": ["year", "month"],
        "broadcast_small": True,
        "target_files_per_month": 1,
    },

    # Parallel load into DWH (experimental)
    "dwh_parallel": {
        "enabled": False,
        "workers": 4,
        "staging_schema": "stg",
        "staging_unlogged": True,
        "keep_staging": False,
        "sync_commit_off": True,
        "analyze_after": True
    },

    # === DQ thresholds and behavior ===
    "dq": {
        "gate_fail_on": ["dq_silver", "dq_join", "dq_dwh"],

        "silver": {
            "null_pickup_ts_pct_max": 2.5,
            "null_dropoff_ts_pct_max": 2.5,
            "null_pu_zone_id_pct_max": 2.5,
            "null_do_zone_id_pct_max": 2.5,
            "null_payment_type_pct_max": 2.5,
            "zero_distance_pct_max": 2.0,
            "zero_duration_pct_max": 2.0,
            "negative_total_pct_max": 1.5,
            "drop_lt_pickup_pct_max": 0.7,
            "duplicate_key_pct_max": 1.5,
        },
        "join": {
            "unmapped_vendor_name_pct_max": 3.0,
            "unmapped_ratecode_desc_pct_max": 3.0,
            "unmapped_payment_desc_pct_max": 3.0,
            "unmapped_pu_zone_name_pct_max": 3.0,
            "unmapped_do_zone_name_pct_max": 3.0,
        },
        "dwh": {
            "rowcount_mismatch_pct_max": 0.5,
            "null_zone_id_pct_max": 2.0,
            "null_payment_type_pct_max": 2.0,
            "null_ratecode_id_pct_max": 2.0,
            "null_vendor_id_pct_max": 2.0,
            "negative_total_pct_max": 1.5,
        }
    }
}
