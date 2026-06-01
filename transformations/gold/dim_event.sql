CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim event - gold layer" AS
SELECT DISTINCT
  event_id,
  event_name,
  event_type,
  year_of_event AS event_year,
  event_start_date AS start_date,
  event_end_date AS end_date,
  `event_distance/length` AS distance,
  event_number_of_finishers AS number_finishers
FROM
  marathos.silver.marathos_obt;