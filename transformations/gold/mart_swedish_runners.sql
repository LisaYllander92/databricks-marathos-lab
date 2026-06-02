USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW  mart_swedish_runners
  COMMENT "Mart for swedish runners for events between 2010 and 2022 - gold layer" AS
SELECT 
a.athlete_id,
a.age_category,
a.birth_year,
a.club,
a.gender,
e.event_name,
e.event_type,
e.event_year,
e.start_date,
e.end_date,
e.distance,
f.performance_seconds,
f.performance_km,
f.average_speed
FROM dim_athlete a
LEFT JOIN fct_results f ON a.athlete_id = f.athlete_id
LEFT JOIN dim_event e ON f.event_id = e.event_id
WHERE a.country_name ='Sweden'
AND e.event_year BETWEEN 2010 AND 2022
ORDER BY e.event_year;