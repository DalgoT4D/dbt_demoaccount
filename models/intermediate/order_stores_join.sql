--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (

SELECT "t1"."_airbyte_extracted_at",
"t1"."_airbyte_meta",
"t1"."_airbyte_raw_id",
"t1"."id",
"t1"."name",
"t1"."opened_at",
"t1"."tax_rate",
"t2"."id" AS "id_2",
"t2"."customer",
"t2"."store_id",
"t2"."subtotal",
"t2"."tax_paid",
"t2"."ordered_at",
"t2"."order_total",
"t2"."_airbyte_raw_id" AS "_airbyte_raw_id_2",
"t2"."_airbyte_extracted_at" AS "_airbyte_extracted_at_2",
"t2"."_airbyte_meta" AS "_airbyte_meta_2"
 FROM {{source('staging', 'raw_stores')}} t1
 LEFT JOIN {{source('staging', 'raw_orders')}} t2
 ON "t1"."id" = "t2"."customer"
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1