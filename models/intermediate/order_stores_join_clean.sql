--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT "customer", "store_id", "id", "subtotal", "ordered_at", "order_total", "opened_at", "name"
FROM {{ ref('order_stores_join') }}
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1