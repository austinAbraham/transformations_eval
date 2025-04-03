{% set report_date = get_report_date() %}

WITH leads AS (
    SELECT
        customer_id,
        report_date,
        lead_id,
        experian_resultset_id,
        lead_type,
        rpl_id
    FROM {{ ref('stg_reporting__leads') }}
    WHERE 
        DATE_TRUNC('MONTH', report_date) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, {{ report_date }}))
        AND LOWER(product_provider) <> 'bmcs' -- Excl. dupes 
)

SELECT * FROM leads