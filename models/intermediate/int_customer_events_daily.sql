WITH daily_events AS (
    SELECT
        customer_id,
        DATE(event_datetime) AS event_date,
        event_category,
        event_type,
        platform,
        COUNT(*) AS event_count,
        SUM(revenue) AS total_revenue
    FROM {{ ref('stg_events') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM daily_events