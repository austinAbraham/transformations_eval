WITH subscription_changes AS (
    SELECT
        customer_id,
        DATE_TRUNC('day', event_datetime) AS event_date,
        event_category,
        event_type,
        CASE 
            WHEN event_category = 'upsell' THEN 'upgrade'
            WHEN event_category = 'downsell' THEN 'downgrade'
            ELSE 'other'
        END AS change_type,
        LAG(event_type) OVER (PARTITION BY customer_id ORDER BY event_datetime) AS previous_product,
        DATEDIFF('day', 
            LAG(event_datetime) OVER (PARTITION BY customer_id ORDER BY event_datetime), 
            event_datetime) AS days_since_last_change
    FROM {{ ref('stg_events') }}
    WHERE event_category IN ('upsell', 'downsell')
)

SELECT
    event_date,
    change_type,
    COUNT(*) AS total_changes,
    AVG(days_since_last_change) AS avg_days_between_changes,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM subscription_changes
GROUP BY 1, 2