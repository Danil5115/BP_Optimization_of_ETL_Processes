CREATE OR REPLACE VIEW svc.v_e5 AS
SELECT *
FROM svc.bench_results
WHERE year = 2021
  AND month = 1
  AND experiment_id IN (
    'E5L_scsv_scopy',           -- single CSV + single COPY
    'E5M_pparts_merge_scopy',   -- parallel write → merge → single COPY
    'E5L_pparts_pcopy'          -- parallel parts + parallel COPY
  );


WITH total AS (
  SELECT experiment_id, SUM(wall_sec) AS wall_sec
  FROM svc.v_e5
  GROUP BY experiment_id
),
base AS (
  SELECT wall_sec AS base_sec
  FROM total
  WHERE experiment_id = 'E5L_scsv_scopy'
)
SELECT
  t.experiment_id,
  ROUND(t.wall_sec::numeric, 3)                                AS pipeline_sec,
  ROUND((t.wall_sec - b.base_sec)::numeric, 3)                 AS delta_vs_base_sec,
  ROUND(100.0*(t.wall_sec - b.base_sec)/NULLIF(b.base_sec,0),2) AS delta_vs_base_pct
FROM total t CROSS JOIN base b
ORDER BY pipeline_sec ASC;



WITH etl AS (
  SELECT experiment_id, SUM(wall_sec) AS etl_sec
  FROM svc.v_e5
  WHERE step IN ('bronze','silver_prep','silver_join')
  GROUP BY experiment_id
),
export AS (
  SELECT experiment_id, SUM(wall_sec) AS export_sec
  FROM svc.v_e5
  WHERE step = 'export'
  GROUP BY experiment_id
),
dwh AS (
  SELECT experiment_id, SUM(wall_sec) AS dwh_sec
  FROM svc.v_e5
  WHERE step IN ('dwh_single','dwh_parallel')
  GROUP BY experiment_id
)
SELECT
  e.experiment_id,
  ROUND(e.etl_sec::numeric,    3) AS etl_sec,
  ROUND(x.export_sec::numeric, 3) AS export_sec,
  ROUND(d.dwh_sec::numeric,    3) AS dwh_sec,
  ROUND((e.etl_sec + x.export_sec + d.dwh_sec)::numeric, 3) AS pipeline_sec
FROM etl e
JOIN export x USING (experiment_id)
JOIN dwh d    USING (experiment_id)
ORDER BY pipeline_sec ASC;


WITH d AS (
  SELECT
    experiment_id,
    step AS dwh_step,
    ROUND(SUM(wall_sec)::numeric, 3)           AS dwh_wall_sec,
    ROUND(MAX(copy_throughput_mib_s)::numeric, 3)  AS copy_mib_s,
    ROUND(MAX(copy_throughput_rows_s)::numeric, 1) AS copy_rows_s,
    ROUND(MAX(table_bytes_mb)::numeric, 3)     AS table_mb,
    ROUND(MAX(index_bytes_mb)::numeric, 3)     AS index_mb
  FROM svc.v_e5
  WHERE step IN ('dwh_single','dwh_parallel')
  GROUP BY experiment_id, step
)
SELECT *
FROM d
ORDER BY dwh_wall_sec ASC;




WITH exp AS (
  SELECT
    experiment_id,
    ROUND(SUM(bytes_out_mb)::numeric, 3) AS bytes_out_mb,
    COALESCE(MAX(files_out),0)           AS files_out,
    ROUND(SUM(write_sec)::numeric, 3)    AS write_sec,
    ROUND(SUM(merge_sec)::numeric, 3)    AS merge_sec
  FROM svc.v_e5
  WHERE step = 'export'
  GROUP BY experiment_id
)
SELECT
  experiment_id,
  files_out,
  bytes_out_mb,
  write_sec,
  merge_sec,
  CASE WHEN write_sec > 0 THEN ROUND(bytes_out_mb / write_sec, 3) END AS export_throughput_mib_s
FROM exp
ORDER BY export_throughput_mib_s DESC NULLS LAST;
