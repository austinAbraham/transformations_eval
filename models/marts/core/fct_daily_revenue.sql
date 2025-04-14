WITH subscription_revenue AS (
    SELECT
        DATE_TRUNC('day', s.start_date) AS revenue_date,
        'subscription' AS revenue_type,
        s.product_id,
        p.product_family,
        COUNT(DISTINCT s.customer_id) AS customer_count,
        SUM(s.monthly_amount) AS daily_revenue
    FROM {{ ref('stg_subscriptions') }} s
    JOIN {{ ref('stg_products') }} p
        ON s.product_id = p.product_id
    WHERE s.status = 'active'
    GROUP BY 1, 2, 3, 4
),

marketplace_revenue AS (
    SELECT
        DATE_TRUNC('day', e.event_datetime) AS revenue_date,
        'marketplace' AS revenue_type,
        'mp_lead' AS product_id,
        'marketplace' AS product_family,
        COUNT(DISTINCT e.customer_id) AS customer_count,
        SUM(e.revenue) AS daily_revenue
    FROM {{ ref('stg_events') }} e
    WHERE e.event_category = 'MP Lead'
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM subscription_revenue
UNION ALL
SELECT * FROM marketplace_revenue