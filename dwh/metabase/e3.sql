CREATE OR REPLACE VIEW svc.v_e3 AS
SELECT *
FROM svc.bench_results
WHERE year = 2021
  AND month = 1
  AND experiment_id IN (
    'E3_sp32_mpb128',
    'E3_sp32_mpb256',
    'E3_sp128_mpb128',
    'E3_sp128_mpb256'
  );


WITH total AS (
  SELECT experiment_id, SUM(wall_sec) AS wall_sec
  FROM svc.v_e3
  GROUP BY experiment_id
),
base AS (
  SELECT wall_sec AS base_sec
  FROM total
  WHERE experiment_id = 'E3_sp32_mpb128'
)
SELECT
  t.experiment_id,
  ROUND(t.wall_sec::numeric, 3)                               AS pipeline_sec,
  ROUND((t.wall_sec - b.base_sec)::numeric, 3)                AS delta_vs_base_sec,
  ROUND(100.0*(t.wall_sec - b.base_sec)/NULLIF(b.base_sec,0), 2) AS delta_vs_base_pct
FROM total t CROSS JOIN base b
ORDER BY pipeline_sec ASC;





WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e3
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




WITH spark_only AS (
  SELECT experiment_id, SUM(wall_sec) AS spark_sec
  FROM svc.v_e3
  WHERE step IN ('bronze','silver_prep','silver_join')
  GROUP BY experiment_id
),
base AS (
  SELECT spark_sec AS base_spark_sec
  FROM spark_only
  WHERE experiment_id = 'E3_sp32_mpb128'
)
SELECT
  s.experiment_id,
  ROUND(s.spark_sec::numeric, 3)                                   AS spark_sec,
  ROUND((s.spark_sec - b.base_spark_sec)::numeric, 3)              AS delta_vs_base_sec,
  ROUND(100.0*(s.spark_sec - b.base_spark_sec)/NULLIF(b.base_spark_sec,0), 2) AS delta_vs_base_pct
FROM spark_only s CROSS JOIN base b
ORDER BY spark_sec ASC;




WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e3
  GROUP BY experiment_id, step
),
pivot AS (
  SELECT
    s.step,
    COALESCE(MAX(CASE WHEN s.experiment_id='E3_sp32_mpb128'  THEN s.wall_sec END),0) AS sp32_mpb128,
    COALESCE(MAX(CASE WHEN s.experiment_id='E3_sp32_mpb256'  THEN s.wall_sec END),0) AS sp32_mpb256,
    COALESCE(MAX(CASE WHEN s.experiment_id='E3_sp128_mpb128' THEN s.wall_sec END),0) AS sp128_mpb128,
    COALESCE(MAX(CASE WHEN s.experiment_id='E3_sp128_mpb256' THEN s.wall_sec END),0) AS sp128_mpb256
  FROM step_agg s
  GROUP BY s.step
),
pipeline AS (
  SELECT
    'pipeline_total' AS step,
    COALESCE(MAX(CASE WHEN experiment_id='E3_sp32_mpb128'  THEN SUM END),0) AS sp32_mpb128,
    COALESCE(MAX(CASE WHEN experiment_id='E3_sp32_mpb256'  THEN SUM END),0) AS sp32_mpb256,
    COALESCE(MAX(CASE WHEN experiment_id='E3_sp128_mpb128' THEN SUM END),0) AS sp128_mpb128,
    COALESCE(MAX(CASE WHEN experiment_id='E3_sp128_mpb256' THEN SUM END),0) AS sp128_mpb256
  FROM (
    SELECT experiment_id, SUM(wall_sec) AS SUM
    FROM svc.v_e3
    GROUP BY experiment_id
  ) t
)
SELECT
  step,
  ROUND(sp32_mpb128::numeric, 3)  AS sp32_mpb128,
  ROUND(sp32_mpb256::numeric, 3)  AS sp32_mpb256,
  ROUND(sp128_mpb128::numeric, 3) AS sp128_mpb128,
  ROUND(sp128_mpb256::numeric, 3) AS sp128_mpb256
FROM (
  SELECT * FROM pivot
  UNION ALL
  SELECT * FROM pipeline
) z
ORDER BY array_position(ARRAY['bronze','silver_prep','silver_join','export','dwh_single','pipeline_total'], step);





WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e3
  WHERE step IN ('bronze','silver_prep','silver_join')
  GROUP BY experiment_id, step
),
base AS (
  SELECT step, wall_sec AS base_sec
  FROM step_agg
  WHERE experiment_id='E3_sp32_mpb128'
)
SELECT
  s.step,
  ROUND(MAX(CASE WHEN s.experiment_id='E3_sp32_mpb128'  THEN s.wall_sec END)::numeric, 3) AS sp32_mpb128,
  ROUND(MAX(CASE WHEN s.experiment_id='E3_sp32_mpb256'  THEN s.wall_sec END)::numeric, 3) AS sp32_mpb256,
  ROUND(MAX(CASE WHEN s.experiment_id='E3_sp128_mpb128' THEN s.wall_sec END)::numeric, 3) AS sp128_mpb128,
  ROUND(MAX(CASE WHEN s.experiment_id='E3_sp128_mpb256' THEN s.wall_sec END)::numeric, 3) AS sp128_mpb256,
  ROUND(100.0 * (MAX(CASE WHEN s.experiment_id='E3_sp128_mpb128' THEN s.wall_sec END) - b.base_sec)
        / NULLIF(b.base_sec,0), 2) AS sp128_mpb128_vs_base_pct
FROM step_agg s
JOIN base b USING (step)
GROUP BY s.step, b.base_sec
ORDER BY array_position(ARRAY['bronze','silver_prep','silver_join'], s.step);
