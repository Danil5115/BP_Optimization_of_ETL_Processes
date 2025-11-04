CREATE OR REPLACE VIEW svc.v_e1 AS
SELECT *
FROM svc.bench_results
WHERE experiment_id IN ('E1_aqe_off','E1_aqe_on')
  AND year = 2021 AND month = 1;

WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e1
  GROUP BY experiment_id, step
),
pivot AS (
  SELECT
    s.step,
    COALESCE(MAX(CASE WHEN s.experiment_id='E1_aqe_off' THEN s.wall_sec END),0) AS off_sec,
    COALESCE(MAX(CASE WHEN s.experiment_id='E1_aqe_on'  THEN s.wall_sec END),0) AS on_sec
  FROM step_agg s
  GROUP BY s.step
),
pipeline AS (
  SELECT
    'pipeline_total' AS step,
    COALESCE(MAX(CASE WHEN experiment_id='E1_aqe_off' THEN SUM END),0) AS off_sec,
    COALESCE(MAX(CASE WHEN experiment_id='E1_aqe_on'  THEN SUM END),0) AS on_sec
  FROM (
    SELECT experiment_id, SUM(wall_sec) AS SUM
    FROM svc.v_e1
    GROUP BY experiment_id
  ) t
)
SELECT
  step,
  ROUND(off_sec::numeric, 3) AS off_sec,
  ROUND(on_sec::numeric, 3)  AS on_sec,
  ROUND((on_sec - off_sec)::numeric, 3) AS delta_sec,
  ROUND(100.0*(on_sec - off_sec)/NULLIF(off_sec,0), 2) AS delta_pct
FROM (
  SELECT * FROM pivot
  UNION ALL
  SELECT * FROM pipeline
) z
ORDER BY array_position(ARRAY['bronze','silver_prep','silver_join','export','dwh_single','pipeline_total'], step);


WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e1
  GROUP BY experiment_id, step
)
SELECT
  step,
  ROUND(MAX(CASE WHEN experiment_id='E1_aqe_off' THEN wall_sec END)::numeric, 3) AS off_sec,
  ROUND(MAX(CASE WHEN experiment_id='E1_aqe_on'  THEN wall_sec END)::numeric, 3) AS on_sec
FROM step_agg
GROUP BY step
ORDER BY array_position(ARRAY['bronze','silver_prep','silver_join','export','dwh_single'], step);


SELECT experiment_id,
       SUM(tasks_total) AS tasks_total
FROM svc.v_e1
WHERE step = 'silver_join'
GROUP BY experiment_id
ORDER BY experiment_id;


SELECT experiment_id,
       ROUND(AVG(skew_factor)::numeric, 3) AS skew_factor_avg
FROM svc.v_e1
WHERE step = 'silver_join'
GROUP BY experiment_id
ORDER BY experiment_id;


SELECT experiment_id,
       ROUND(MAX(copy_throughput_mib_s)::numeric, 3)  AS mib_s,
       ROUND(MAX(copy_throughput_rows_s)::numeric, 1) AS rows_s
FROM svc.v_e1
WHERE step = 'dwh_single'
GROUP BY experiment_id
ORDER BY experiment_id;
