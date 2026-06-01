CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
    result_id,
    event_id,
    athlete_id,
    performance_seconds AS performance,
    athlete_average_speed AS average_speed
FROM
  STREAM marathos.silver.marathos_obt;