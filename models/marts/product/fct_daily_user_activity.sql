WITH daily_activity AS (
    SELECT
        DATE(event_datetime) AS activity_date,
        platform,
        event_category,
        COUNT(DISTINCT customer_id) AS unique_users,
        COUNT(*) AS total_events,
        AVG(COUNT(*)) OVER (PARTITION BY customer_id) AS avg_events_per_user
    FROM {{ ref('stg_events') }}
    GROUP BY 1, 2, 3
)

SELECT * FROM daily_activity