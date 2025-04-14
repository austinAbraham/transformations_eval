-- Test to ensure no duplicate customer_id + report_date combinations
SELECT
    customer_id,
    report_date,
    COUNT(*) as row_count
FROM {{ ref('mart_marketplace__customer_kpis') }}
GROUP BY customer_id, report_date
HAVING COUNT(*) > 1