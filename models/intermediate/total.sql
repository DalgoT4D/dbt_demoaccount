{{ config(
  materialized='table'
) }}

SELECT "Country_Region",
       SUM(CAST("Confirmed" AS INTEGER)) AS Total_Confirmed,
       SUM(CAST("Recovered" AS INTEGER)) AS Total_Recovered,
       SUM(CAST("Active" AS INTEGER)) AS Total_Active,
       SUM(CAST("Deaths" AS INTEGER)) AS Total_Deaths
FROM {{ source('source_demo', 'covid19') }}
GROUP BY "Country_Region"
ORDER BY Total_Confirmed DESC

