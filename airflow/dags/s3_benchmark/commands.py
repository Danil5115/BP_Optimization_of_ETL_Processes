from __future__ import annotations

# Jinja2 spark-submit commands

bronze_cmd = r"""
{% set RC  = dag_run.conf if dag_run else {} %}
{% set DC  = RC.get('default_conf', {}) %}
{% set OPT = (RC.get('opt') or DC.get('opt') or params.default_conf.opt) %}
{% set MODE = RC.get('mode', 'optimized') %}
{% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
{% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
{% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
{% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}
{% set DATASET = RC.get('dataset', DC.get('dataset', params.default_conf.dataset)) %}
{% set TFILES = (1 if MODE=='baseline' else OPT.target_files_per_month) | int %}

/opt/spark/bin/spark-submit /opt/etl/spark/jobs/bronze_csv_to_parquet.py
--dataset {{ DATASET }} --year {{ YEAR }} --month {{ MONTH }}
--data-root {{ var.value.DATA_ROOT }} --experiment-id {{ EXP }} --trial {{ TRIAL }}
--parquet-codec {{ OPT.parquet_codec }} --shuffle-partitions {{ OPT.shuffle_partitions }}
--max-partition-bytes {{ OPT.max_partition_bytes }} --target-files-per-month {{ TFILES }}
{% if OPT.aqe %}--aqe{% endif %} {% if OPT.broadcast_small %}--broadcast-small{% endif %}
"""

silver_cmd = r"""
{% set RC  = dag_run.conf if dag_run else {} %}
{% set DC  = RC.get('default_conf', {}) %}
{% set OPT = (RC.get('opt') or DC.get('opt') or params.default_conf.opt) %}
{% set MODE = RC.get('mode', 'optimized') %}
{% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
{% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
{% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
{% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}
{% set TFILES = (1 if MODE=='baseline' else OPT.target_files_per_month) | int %}

/opt/spark/bin/spark-submit /opt/etl/spark/jobs/silver_trips_basic.py
--year {{ YEAR }} --month {{ MONTH }} --data-root {{ var.value.DATA_ROOT }}
--experiment-id {{ EXP }} --trial {{ TRIAL }}
--parquet-codec {{ OPT.parquet_codec }} --shuffle-partitions {{ OPT.shuffle_partitions }}
--max-partition-bytes {{ OPT.max_partition_bytes }} --target-files-per-month {{ TFILES }}
{% if OPT.aqe %}--aqe{% endif %}
"""

silver_join_cmd = """
{% set RC  = dag_run.conf if dag_run else {} %}
{% set DC  = RC.get('default_conf', {}) %}
{% set OPT = (RC.get('opt') or DC.get('opt') or params.default_conf.opt) %}
{% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
{% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
{% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
{% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}

 /opt/spark/bin/spark-submit /opt/etl/spark/jobs/silver_join.py
 --year {{ YEAR }} --month {{ MONTH }} --data-root {{ var.value.DATA_ROOT }}
 --experiment-id {{ EXP }} --trial {{ TRIAL }}
 --parquet-codec {{ OPT.parquet_codec }} --shuffle-partitions {{ OPT.shuffle_partitions|int }}
 --max-partition-bytes {{ OPT.max_partition_bytes|int }} {% if OPT.aqe %}--aqe{% endif %}
 {% if OPT.broadcast_small %}--broadcast-small{% else %}--disable-auto-broadcast{% endif %}
 --zones-path {{ var.value.DATA_ROOT }}/ref/zones/zones.parquet
 --ratecodes-path {{ var.value.DATA_ROOT }}/ref/ratecode/ratecode.parquet
 --payments-path {{ var.value.DATA_ROOT }}/ref/payment_type/payment_type.parquet
 --vendors-path {{ var.value.DATA_ROOT }}/ref/vendor/vendor.parquet
 --storeflag-path {{ var.value.DATA_ROOT }}/ref/store_flag/store_flag.parquet
 --dimdate-path {{ var.value.DATA_ROOT }}/ref/dim_date/dim_date.parquet
"""


export_cmd = r"""
{% set RC  = dag_run.conf if dag_run else {} %}
{% set DC  = RC.get('default_conf', {}) %}
{% set OPT = (RC.get('opt') or DC.get('opt') or params.default_conf.opt) %}
{% set STRAT = RC.get('export_strategy', DC.get('export_strategy', 'single_csv')) %}
{% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
{% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
{% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
{% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}
{% set TFILES = (1 if STRAT == 'single_csv' else (OPT.target_files_per_month | int)) %}

/opt/spark/bin/spark-submit /opt/etl/spark/jobs/export_fact_csv.py
--year {{ YEAR }} --month {{ MONTH }} --data-root {{ var.value.DATA_ROOT }}
--experiment-id {{ EXP }} --trial {{ TRIAL }} --target-files-per-month {{ TFILES }}
--measure-rows {% if STRAT == 'parallel_parts' %} --save-parts --parts-subdir parts {% endif %}
"""

dq_silver_cmd = r"""
{% set RC  = dag_run.conf if dag_run else {} %}
{% set DC  = RC.get('default_conf', {}) %}
{% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
{% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
{% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
{% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}
/opt/spark/bin/spark-submit /opt/etl/spark/jobs/dq_silver.py
--year {{ YEAR }} --month {{ MONTH }} --data-root {{ var.value.DATA_ROOT }} --experiment-id {{ EXP }} --trial {{ TRIAL }}
"""

dq_join_cmd = r"""
{% set RC  = dag_run.conf if dag_run else {} %}
{% set DC  = RC.get('default_conf', {}) %}
{% set YEAR = RC.get('year',  DC.get('year',  params.default_conf.year)) %}
{% set MONTH = RC.get('month', DC.get('month', params.default_conf.month)) %}
{% set EXP = RC.get('experiment_id', DC.get('experiment_id', params.default_conf.experiment_id)) %}
{% set TRIAL = RC.get('trial', DC.get('trial', params.default_conf.trial)) %}
/opt/spark/bin/spark-submit /opt/etl/spark/jobs/dq_join.py
--year {{ YEAR }} --month {{ MONTH }} --data-root {{ var.value.DATA_ROOT }} --experiment-id {{ EXP }} --trial {{ TRIAL }}
"""
