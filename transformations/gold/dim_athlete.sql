CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim athlete - gold layer" AS
SELECT DISTINCT
  athlete_id,
  athlete_country AS country_code,
  country_name,
  athlete_year_of_birth AS birth_year,
  athlete_age_category AS age_category,
  athlete_gender AS gender,
  athlete_club AS club
FROM
  marathos.silver.marathos_obt;