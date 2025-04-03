-- Test to ensure no duplicate customer/date combinations
SELECT
    customer_id,
    report_date,
    COUNT(*) AS row_count
FROM {{ ref('mart_marketplace__customer_kpis') }}
GROUP BY customer_id, report_date
HAVING COUNT(*) > 1