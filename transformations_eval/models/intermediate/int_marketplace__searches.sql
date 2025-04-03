{% set report_date = get_report_date() %}

WITH searches AS (
    SELECT 
        customer_id,
        report_date,
        experian_resultset_id,
        requested_product
    FROM {{ ref('stg_reporting__product_history') }}
    WHERE 
        DATE_TRUNC('MONTH', report_date) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, {{ report_date }}))
        AND requested_product IN ('creditcard', 'loan', 'carfinance', 'carrefinance')
)

SELECT * FROM searches