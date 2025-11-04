CREATE OR REPLACE VIEW svc.v_e4 AS
SELECT *
FROM svc.bench_results
WHERE year = 2021
  AND month = 1
  AND experiment_id IN ('E4_f1','E4_f8');


WITH total AS (
  SELECT experiment_id, SUM(wall_sec) AS wall_sec
  FROM svc.v_e4
  WHERE step IN ('bronze','silver_prep','silver_join','export','dwh_single')
  GROUP BY experiment_id
),
base AS (
  SELECT wall_sec AS base_sec
  FROM total
  WHERE experiment_id = 'E4_f1'
)
SELECT
  t.experiment_id,
  ROUND(t.wall_sec::numeric, 3)                                AS pipeline_sec,
  ROUND((t.wall_sec - b.base_sec)::numeric, 3)                 AS delta_vs_f1_sec,
  ROUND(100.0*(t.wall_sec - b.base_sec)/NULLIF(b.base_sec,0),2) AS delta_vs_f1_pct
FROM total t CROSS JOIN base b
ORDER BY pipeline_sec ASC;


WITH spark_only AS (
  SELECT experiment_id, SUM(wall_sec) AS spark_sec
  FROM svc.v_e4
  WHERE step IN ('bronze','silver_prep','silver_join')
  GROUP BY experiment_id
),
base AS (
  SELECT spark_sec AS base_spark_sec
  FROM spark_only
  WHERE experiment_id = 'E4_f1'
)
SELECT
  s.experiment_id,
  ROUND(s.spark_sec::numeric, 3)                                AS spark_sec,
  ROUND((s.spark_sec - b.base_spark_sec)::numeric, 3)           AS delta_vs_f1_sec,
  ROUND(100.0*(s.spark_sec - b.base_spark_sec)/NULLIF(b.base_spark_sec,0),2) AS delta_vs_f1_pct
FROM spark_only s CROSS JOIN base b
ORDER BY spark_sec ASC;


WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e4
  WHERE step IN ('bronze','silver_prep','silver_join')
  GROUP BY experiment_id, step
)
SELECT
  experiment_id,
  step,
  ROUND(wall_sec::numeric, 3) AS wall_sec
FROM step_agg
ORDER BY
  array_position(ARRAY['bronze','silver_prep','silver_join'], step),
  experiment_id;


SELECT
  step,
  experiment_id,
  ROUND(shuffle_read_mb::numeric, 1)  AS shuffle_read_mb,
  ROUND(shuffle_write_mb::numeric, 1) AS shuffle_write_mb,
  ROUND(disk_spill_mb::numeric, 1)    AS disk_spill_mb,
  ROUND(memory_spill_mb::numeric, 1)  AS memory_spill_mb,
  ROUND(skew_factor::numeric, 2)      AS skew_factor
FROM svc.v_e4
WHERE step IN ('bronze','silver_prep','silver_join')
ORDER BY
  array_position(ARRAY['bronze','silver_prep','silver_join'], step),
  experiment_id;


