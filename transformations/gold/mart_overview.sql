USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_overview AS
SELECT
  f.result_id,
  e.event_name,
  e.event_type,
  e.start_date,
  e.event_year,
  e.distance,
  e.number_finishers,
  f.performance_seconds,
  f.performance_km,
  f.average_speed,
  f.age_category,
  a.athlete_id,
  a.gender,
  a.country_name,
  a.birth_year,
  d.month_name,
  d.quarter,
  d.day_name
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event e ON f.event_id = e.event_id
JOIN marathos.gold.dim_athlete a ON f.athlete_id = a.athlete_id
JOIN marathos.gold.dim_date d ON e.start_date = d.date;