{{ config(
  materialized='table'
) }}

SELECT WHO_Region,
       SUM(Confirmed) AS Total_Confirmed,
       SUM(Deaths) AS Total_Deaths
FROM {{ source('source_demo', 'covid19') }}
GROUP BY WHO_Region
ORDER BY Total_Confirmed DESC;
