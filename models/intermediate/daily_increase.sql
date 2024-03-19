{{ config(
  materialized='table'
) }}

WITH DailyChanges AS (
  SELECT Date,
         Country_Region,
         Confirmed - LAG(Confirmed, 1) OVER(PARTITION BY Country_Region ORDER BY Date) AS Daily_Increase_Confirmed,
         Deaths - LAG(Deaths, 1) OVER(PARTITION BY Country_Region ORDER BY Date) AS Daily_Increase_Deaths
  FROM {{ source('source_demo', 'covid19') }}
)
SELECT Date,
       SUM(Daily_Increase_Confirmed) AS Total_Daily_Increase_Confirmed,
       SUM(Daily_Increase_Deaths) AS Total_Daily_Increase_Deaths
FROM DailyChanges
GROUP BY Date
ORDER BY Date;
