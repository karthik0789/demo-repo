{{ config(materialized='table') }}

WITH source AS 
(
    SELECT 
    customer as customer,
    sum(amount) as amount
    FROM
        {{ source('RAW','SALES_ITEM') }}
    GROUP BY customer
)
select * from source