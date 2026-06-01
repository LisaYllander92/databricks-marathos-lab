CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_distance_events
  COMMENT "Mart for distance events with athlete and country info - gold layer" AS
SELECT
  f.result_id,
  f.performance_seconds,
  f.average_speed,
  e.event_name,
  e.distance,
  e.start_date,
  e.end_date,
  e.number_finishers,
  a.athlete_id,
  a.gender,
  a.age_category,
  a.country_name,
  a.birth_year
FROM
  marathos.gold.fct_results f
    LEFT JOIN marathos.gold.dim_event e
      ON f.event_id = e.event_id
    LEFT JOIN marathos.gold.dim_athlete a
      ON f.athlete_id = a.athlete_id
WHERE
  e.event_type = 'distance';