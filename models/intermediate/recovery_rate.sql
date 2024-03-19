{{ config(
  materialized='table'
) }}

SELECT "Country_Region",
       SUM(CAST("Recovered" AS INTEGER)) / NULLIF(SUM(CAST("Confirmed" AS INTEGER)), 0) * 100 AS Recovery_Rate_Percent
FROM  {{ source('source_demo', 'covid19') }}
GROUP BY "Country_Region"
ORDER BY Recovery_Rate_Percent DESC