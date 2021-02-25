{{ config(materialized='table') }}

WITH source AS 
(
    SELECT 
    salesdoc as salesdoc,
    sum(amount) as amount
    FROM
        {{ source('RAW','SALES_ITEM') }} 
    GROUP BY salesdoc
)
select * from source