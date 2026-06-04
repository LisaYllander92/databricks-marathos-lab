CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
    result_id,
    event_id,
    athlete_id,
    performance_seconds,
    performance_km,
    athlete_average_speed AS average_speed,
    athlete_club AS club,
    athlete_age_category AS age_category
FROM
  STREAM marathos.silver.marathos_obt;