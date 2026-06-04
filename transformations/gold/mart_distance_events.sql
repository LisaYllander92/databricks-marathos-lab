USE CATALOG marathos;

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW mart_distance_events
  COMMENT "Mart for distance events with athlete and country info - gold layer" AS
SELECT
  f.result_id,
  f.performance_seconds,
  f.performance_seconds / 3600.0 AS performance_hours,
  f.average_speed,
  f.age_category,
  e.event_name,
  e.distance,
  e.start_date,
  e.end_date,
  e.event_year,
  e.number_finishers,
  a.athlete_id,
  a.gender,
  a.country_name,
  a.birth_year
FROM
  fct_results f
    JOIN dim_event e
      ON f.event_id = e.event_id
    JOIN dim_athlete a
      ON f.athlete_id = a.athlete_id
WHERE
  e.event_type = 'distance'
  AND a.country_name IN (
    SELECT
      country_name
    FROM
      marathos.gold.dim_athlete
    GROUP BY
      country_name
    HAVING
      COUNT(*) >= 100
  );