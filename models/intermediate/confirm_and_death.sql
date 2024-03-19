{{ config(
  materialized='table'
) }}

SELECT "WHO_Region",
       SUM(CAST("Confirmed" AS INTEGER)) AS Total_Confirmed,
       SUM(CAST("Deaths" AS INTEGER)) AS Total_Deaths
FROM {{ source('source_demo', 'covid19') }}
GROUP BY "WHO_Region"
ORDER BY Total_Confirmed DESC
