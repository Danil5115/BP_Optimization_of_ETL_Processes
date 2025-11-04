\connect dwh de;

-- === Schemas ===
CREATE SCHEMA IF NOT EXISTS dds AUTHORIZATION de;
CREATE SCHEMA IF NOT EXISTS svc AUTHORIZATION de;

-- Optional: base privileges on schemas
GRANT USAGE, CREATE ON SCHEMA dds TO de;
GRANT USAGE, CREATE ON SCHEMA svc TO de;

-- === Dimensions ===
CREATE TABLE IF NOT EXISTS dds.dim_vendor (
  vendor_id   int PRIMARY KEY,
  vendor_name text
);

CREATE TABLE IF NOT EXISTS dds.dim_ratecodes (
  ratecode_id int PRIMARY KEY,
  description text
);

CREATE TABLE IF NOT EXISTS dds.dim_payment_type (
  payment_type_id int PRIMARY KEY,
  description     text
);

CREATE TABLE IF NOT EXISTS dds.dim_store_flag (
  store_flag  text PRIMARY KEY,
  description text
);

CREATE TABLE IF NOT EXISTS dds.dim_zones (
  zone_id       int PRIMARY KEY,
  borough       text,
  zone_name     text,
  service_zone  text
);

CREATE TABLE IF NOT EXISTS dds.dim_date (
  date_id        int PRIMARY KEY,
  "date"         date NOT NULL,
  year           int,
  quarter        int,
  month          int,
  day            int,
  iso_week       int,
  iso_dow        int,
  is_weekend     boolean,
  is_month_start boolean,
  is_month_end   boolean
);

CREATE INDEX IF NOT EXISTS idx_dim_date_date ON dds.dim_date("date");
CREATE INDEX IF NOT EXISTS idx_dim_zones_borough_zone ON dds.dim_zones(borough, zone_name);

-- === Fact (parent only; child partitions are created by dwh.py) ===
DROP TABLE IF EXISTS dds.fact_trips CASCADE;

CREATE TABLE dds.fact_trips (
  trip_id                bigserial,

  vendor_id              int REFERENCES dds.dim_vendor(vendor_id),
  pickup_ts              timestamp,
  dropoff_ts             timestamp,

  pickup_date_id         int REFERENCES dds.dim_date(date_id),
  dropoff_date_id        int REFERENCES dds.dim_date(date_id),

  passenger_count        int,
  trip_distance          double precision,
  ratecode_id            int REFERENCES dds.dim_ratecodes(ratecode_id),
  store_and_fwd_flag     text,
  pu_zone_id             int REFERENCES dds.dim_zones(zone_id),
  do_zone_id             int REFERENCES dds.dim_zones(zone_id),
  payment_type           int REFERENCES dds.dim_payment_type(payment_type_id),
  fare_amount            double precision,
  extra                  double precision,
  mta_tax                double precision,
  tip_amount             double precision,
  tolls_amount           double precision,
  improvement_surcharge  double precision,
  total_amount           double precision,
  congestion_surcharge   double precision,

  year                   int,
  month                  int,

  load_dt                timestamp default now(),
  load_src               text,
  row_hash               text,

  CONSTRAINT chk_store_flag CHECK (store_and_fwd_flag IN ('Y','N','U') OR store_and_fwd_flag IS NULL)
) PARTITION BY RANGE (year, month);

-- === Metrics / Data Quality ===
CREATE TABLE IF NOT EXISTS svc.bench_results (
  experiment_id       text,
  run_id              text,
  mode                text,
  step                text,
  year                int,
  month               int,
  rows_in             bigint,
  rows_out            bigint,
  bytes_in_mb         numeric,
  bytes_out_mb        numeric,
  wall_sec            numeric,
  shuffle_read_mb     numeric,
  shuffle_write_mb    numeric,
  spark_conf          jsonb,
  partitions_in               int,
  partitions_out_planned      int,
  files_out                   int,
  files_in                    int,
  write_sec                   numeric,
  merge_sec                   numeric,
  tasks_total                 int,
  stages                      int,
  disk_spill_mb               numeric,
  memory_spill_mb             numeric,
  gc_time_ms                  bigint,
  executor_run_time_ms        bigint,
  skew_factor                 numeric,
  wal_bytes_mb                numeric,
  table_bytes_mb              numeric,
  index_bytes_mb              numeric,
  copy_sec_sum                numeric,
  copy_sec_max                numeric,
  analyze_sec                 numeric,
  copy_throughput_mib_s       numeric,
  copy_throughput_rows_s      numeric,
  details                     jsonb,
  created_at          timestamp DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_bench_results_main
  ON svc.bench_results (experiment_id, mode, step, year, month);

CREATE TABLE IF NOT EXISTS svc.dq_results (
  experiment_id  text,
  run_id         text,
  step           text,
  year           int,
  month          int,
  rule           text,
  value_num      numeric,
  threshold      numeric,
  status         text,
  details        jsonb,
  created_at     timestamp DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dq_results_main
  ON svc.dq_results (experiment_id, step, year, month, rule);

CREATE OR REPLACE VIEW svc.v_dq_summary AS
SELECT experiment_id, step, year, month,
       COUNT(*)                                   AS rules_count,
       SUM((status='OK')::int)                    AS ok_cnt,
       SUM((status='WARN')::int)                  AS warn_cnt,
       SUM((status='FAIL')::int)                  AS fail_cnt,
       MIN(created_at)                            AS first_at,
       MAX(created_at)                            AS last_at
FROM svc.dq_results
GROUP BY experiment_id, step, year, month;
