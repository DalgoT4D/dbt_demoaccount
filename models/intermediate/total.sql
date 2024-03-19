{{ config(
  materialized='table'
) }}


SELECT Country_Region,
       SUM(Confirmed) AS Total_Confirmed,
       SUM(Recovered) AS Total_Recovered,
       SUM(Active) AS Total_Active,
       SUM(Deaths) AS Total_Deaths
FROM {{ source('source_demo', 'covid19') }}
GROUP BY Country_Region
ORDER BY Total_Confirmed DESC;
