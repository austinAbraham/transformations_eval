WITH customer_revenue AS (
    SELECT
        c.customer_id,
        c.acquisition_channel,
        c.acquisition_type,
        DATE_TRUNC('month', c.acquisition_date) AS acquisition_month,
        SUM(s.monthly_amount) AS total_subscription_revenue,
        SUM(e.revenue) AS total_marketplace_revenue,
        DATEDIFF('month', c.acquisition_date, CURRENT_DATE) AS months_since_acquisition,
        DATEDIFF('month', c.acquisition_date, COALESCE(MAX(s.end_date), CURRENT_DATE)) AS customer_lifetime_months
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('stg_subscriptions') }} s
        ON c.customer_id = s.customer_id
    LEFT JOIN {{ ref('stg_events') }} e
        ON c.customer_id = e.customer_id AND e.event_category = 'MP Lead'
    GROUP BY 1, 2, 3, 4
)

SELECT
    customer_id,
    acquisition_channel,
    acquisition_type,
    acquisition_month,
    total_subscription_revenue,
    total_marketplace_revenue,
    (total_subscription_revenue + total_marketplace_revenue) AS total_revenue,
    customer_lifetime_months,
    CASE 
        WHEN customer_lifetime_months > 0 
        THEN (total_subscription_revenue + total_marketplace_revenue) / customer_lifetime_months
        ELSE 0
    END AS monthly_ltv
FROM customer_revenue