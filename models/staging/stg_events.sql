WITH source AS (
    SELECT * FROM {{ source('raw', 'events') }}
),

renamed AS (
    SELECT
        event_guid,
        customer_id,
        event_type,
        event_category,
        event_datetime,
        platform,
        CAST(revenue AS DECIMAL(10,2)) AS revenue
    FROM source
)

SELECT * FROM renamed