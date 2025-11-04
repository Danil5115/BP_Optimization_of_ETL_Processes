CREATE OR REPLACE VIEW svc.v_e2 AS
SELECT *
FROM svc.bench_results
WHERE year = 2021 AND month = 1
  AND experiment_id IN (
    'E2_broadcast_off__2021_01_t1',
    'E2_broadcast_on__2021_01_t1'
  );


WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e2
  GROUP BY experiment_id, step
),
pivot AS (
  SELECT
    s.step,
    COALESCE(MAX(CASE WHEN s.experiment_id='E2_broadcast_off__2021_01_t1' THEN s.wall_sec END),0) AS off_sec,
    COALESCE(MAX(CASE WHEN s.experiment_id='E2_broadcast_on__2021_01_t1'  THEN s.wall_sec END),0) AS on_sec
  FROM step_agg s
  GROUP BY s.step
),
pipeline AS (
  SELECT
    'pipeline_total' AS step,
    COALESCE(MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN SUM END),0) AS off_sec,
    COALESCE(MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN SUM END),0) AS on_sec
  FROM (
    SELECT experiment_id, SUM(wall_sec) AS SUM
    FROM svc.v_e2
    GROUP BY experiment_id
  ) t
)
SELECT
  step,
  ROUND(off_sec::numeric, 3) AS off_sec,
  ROUND(on_sec::numeric, 3)  AS on_sec,
  ROUND((on_sec - off_sec)::numeric, 3) AS delta_sec,
  ROUND(100.0*(on_sec - off_sec)/NULLIF(off_sec,0), 2) AS delta_pct  -- тот же знак, как в E1
FROM (
  SELECT * FROM pivot
  UNION ALL
  SELECT * FROM pipeline
) z
ORDER BY array_position(ARRAY['bronze','silver_prep','silver_join','export','dwh_single','pipeline_total'], step);


WITH step_agg AS (
  SELECT experiment_id, step, SUM(wall_sec) AS wall_sec
  FROM svc.v_e2
  GROUP BY experiment_id, step
)
SELECT
  step,
  ROUND(MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN wall_sec END)::numeric, 3) AS off_sec,
  ROUND(MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN wall_sec END)::numeric, 3) AS on_sec
FROM step_agg
GROUP BY step
ORDER BY array_position(ARRAY['bronze','silver_prep','silver_join','export','dwh_single'], step);


SELECT
  CASE
    WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN 'off'
    WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN 'on'
  END AS variant,
  ROUND(shuffle_read_mb::numeric, 1)  AS shuffle_read_mb,
  ROUND(shuffle_write_mb::numeric, 1) AS shuffle_write_mb,
  ROUND(disk_spill_mb::numeric, 1)    AS disk_spill_mb,
  ROUND(memory_spill_mb::numeric, 1)  AS memory_spill_mb
FROM svc.v_e2
WHERE step='silver_join'
ORDER BY variant;


WITH j AS (
  SELECT
    experiment_id,
    SUM(wall_sec)                AS wall_sec,
    SUM(executor_run_time_ms)    AS exec_ms,
    SUM(gc_time_ms)              AS gc_ms,
    SUM(tasks_total)             AS tasks_total,
    MAX(stages)                  AS stages
  FROM svc.v_e2
  WHERE step = 'silver_join'
  GROUP BY experiment_id
),
p AS (
  SELECT
    MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN wall_sec  END) AS off_wall_sec,
    MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN wall_sec  END) AS on_wall_sec,
    MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN exec_ms   END) AS off_exec_ms,
    MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN exec_ms   END) AS on_exec_ms,
    MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN gc_ms     END) AS off_gc_ms,
    MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN gc_ms     END) AS on_gc_ms,
    MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN tasks_total END) AS off_tasks,
    MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN tasks_total END) AS on_tasks,
    MAX(CASE WHEN experiment_id='E2_broadcast_off__2021_01_t1' THEN stages    END) AS off_stages,
    MAX(CASE WHEN experiment_id='E2_broadcast_on__2021_01_t1'  THEN stages    END) AS on_stages
  FROM j
)
SELECT metric,
       ROUND(off_val::numeric, 3) AS off,
       ROUND(on_val::numeric, 3)  AS on,
       ROUND(100.0*(off_val - on_val)/NULLIF(off_val,0), 1) AS improvement_pct
FROM (
  SELECT 'wall_sec'     AS metric, off_wall_sec                    AS off_val, on_wall_sec                    AS on_val FROM p
  UNION ALL
  SELECT 'executor_sec'          , off_exec_ms/1000.0              , on_exec_ms/1000.0              FROM p
  UNION ALL
  SELECT 'gc_sec'                 , off_gc_ms/1000.0                , on_gc_ms/1000.0                FROM p
  UNION ALL
  SELECT 'tasks_total'            , off_tasks::numeric              , on_tasks::numeric              FROM p
  UNION ALL
  SELECT 'stages'                 , off_stages::numeric             , on_stages::numeric             FROM p
) x;