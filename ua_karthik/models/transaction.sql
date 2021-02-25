{{ config(
    materialized = 'table'
) }}

WITH stylemaster AS (

    SELECT
        stylenumber,
        gender
    FROM
        {{ source(
            'RAW',
            'STYLE_MASTER'
        ) }}
),
stylecolor AS (
    SELECT
        stylenumber,
        colorcode,
        SIZE
    FROM
        {{ source(
            'RAW',
            'STYLE_COLOR'
        ) }}
),
custtran AS (
    SELECT
        transactionid,
        customernumber,
        transactiondate,
        stylenumber,
        quantity,
        amount
    FROM
        {{ source(
            'RAW',
            'CUST_TRANSACTION'
        ) }}
),
FINAL AS (
    SELECT
        custtran.customernumber,
        custtran.stylenumber,
        custtran.transactiondate,
        SUM(custtran.amount)
    FROM
        custtran
        LEFT JOIN stylemaster
        ON custtran.stylenumber = stylemaster.stylenumber
        LEFT JOIN stylecolor
        ON custtran.stylenumber = stylecolor.stylenumber
    GROUP BY
        custtran.customernumber,
        custtran.stylenumber,
        custtran.transactiondate
)
SELECT
    *
FROM
    FINAL
