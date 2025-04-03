WITH marketplace_data AS (
    SELECT
        customer_id,
        report_date,
        requested_product,
        experian_resultset_id,
        lead_id,
        lead_type,
        revenue
    FROM {{ ref('int_marketplace__searches_leads_with_revenue') }}
    
    {% if is_incremental() %}
    WHERE report_date >= (SELECT MAX(report_date) FROM {{ this }})
    {% endif %}
),

marketplace_aggregated AS (
    SELECT 
        customer_id,
        report_date,
        requested_product,
        COUNT(DISTINCT experian_resultset_id) AS unique_searches,
        COUNT(DISTINCT CASE WHEN lead_type = 0 THEN lead_id ELSE NULL END) AS unique_zero_elig_leads,
        COUNT(DISTINCT CASE WHEN lead_type = 1 THEN lead_id ELSE NULL END) AS unique_regular_leads,
        SUM(CASE WHEN lead_type = 0 THEN revenue ELSE 0 END) AS zero_elig_revenue,
        SUM(CASE WHEN lead_type = 1 THEN revenue ELSE 0 END) AS regular_revenue
    FROM marketplace_data
    GROUP BY customer_id, report_date, requested_product
)

SELECT 
    report_date,
    customer_id,
    SUM(unique_searches) AS marketplace_searches,
    SUM(unique_zero_elig_leads + unique_regular_leads) AS marketplace_leads,
    SUM(zero_elig_revenue + regular_revenue) AS marketplace_revenue,
    SUM(unique_regular_leads) AS marketplace_leads_regular,
    SUM(regular_revenue) AS marketplace_revenue_regular,
    SUM(unique_zero_elig_leads) AS marketplace_leads_zero_elig,
    SUM(zero_elig_revenue) AS marketplace_revenue_zero_elig,
    -- Product-specific metrics follow
    SUM(CASE WHEN requested_product = 'carfinance' THEN unique_searches ELSE 0 END) AS marketplace_searches_car_finance,
    SUM(CASE WHEN requested_product = 'carrefinance' THEN unique_searches ELSE 0 END) AS marketplace_searches_car_re_finance,
    -- ... additional product-specific metrics ...
FROM marketplace_aggregated
GROUP BY report_date, customer_id