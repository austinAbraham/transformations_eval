WITH acquisition_events AS (
    SELECT
        e.customer_id,
        e.event_datetime AS acquisition_date,
        e.event_type AS acquisition_type,
        SPLIT_PART(e.event_type, '_', 3) AS acquisition_channel,
        c.customer_age,
        c.customer_age_band,
        ROW_NUMBER() OVER (PARTITION BY e.customer_id ORDER BY e.event_datetime) AS event_sequence
    FROM {{ ref('stg_events') }} e
    JOIN {{ ref('stg_customers') }} c
        ON e.customer_id = c.customer_id
    WHERE e.event_category = 'acquisition'
)

SELECT 
    customer_id,
    acquisition_date,
    acquisition_type,
    acquisition_channel,
    customer_age,
    customer_age_band
FROM acquisition_events
WHERE event_sequence = 1