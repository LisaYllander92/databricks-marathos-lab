CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
  COMMENT "Dim athlete - gold layer" AS
SELECT DISTINCT
  event_start_date AS date,
  year(event_start_date) AS year,
  month(event_start_date) AS month,
  dayofmonth(event_start_date) AS day,
  quarter(event_start_date) AS quarter,
  dayofweek(event_start_date) AS day_of_week,
  date_format(event_start_date, 'MMMM') AS month_name,
  date_format(event_start_date, 'EEEE') AS day_name
FROM
  marathos.silver.marathos_obt
WHERE
  event_start_date IS NOT NULL;