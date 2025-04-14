WITH login_metrics AS (
    SELECT
        DATE_TRUNC('day', event_datetime) AS activity_date,
        p.product_family,
        COUNT(DISTINCT e.customer_id) AS unique_users,
        COUNT(*) AS total_logins,
        COUNT(*) / COUNT(DISTINCT e.customer_id) AS logins_per_user
    FROM {{ ref('stg_events') }} e
    JOIN {{ ref('stg_subscriptions') }} s
        ON e.customer_id = s.customer_id
        AND e.event_datetime BETWEEN s.start_date AND COALESCE(s.end_date, CURRENT_DATE)
    JOIN {{ ref('stg_products') }} p
        ON s.product_id = p.product_id
    WHERE e.event_category = 'login'
    GROUP BY 1, 2
)

SELECT * FROM login_metrics