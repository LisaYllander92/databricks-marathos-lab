CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim event - gold layer" AS
SELECT
  event_id,
  MIN(event_name) AS event_name,
  MIN(event_type) AS event_type,
  MIN(year_of_event) AS event_year,
  MIN(event_start_date) AS start_date,
  MIN(event_end_date) AS end_date,
  MIN(`event_distance/length`) AS distance,
  MAX(event_number_of_finishers) AS number_finishers
FROM
  marathos.silver.marathos_obt
GROUP BY 
  event_id;