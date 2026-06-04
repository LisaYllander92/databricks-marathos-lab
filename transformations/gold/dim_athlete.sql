CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim athlete - gold layer" AS
SELECT 
  athlete_id,
  MIN(athlete_country) AS country_code,
  MIN(country_name) AS country_name,
  MIN(athlete_year_of_birth) AS birth_year,
  MIN(athlete_gender) AS gender
FROM
  marathos.silver.marathos_obt
  GROUP BY athlete_id;