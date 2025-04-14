WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        customer_age,
        customer_age_band,
        customer_is_prospect,
        date_created AS effective_date,
        CASE WHEN current_flag = 'Y' THEN TRUE ELSE FALSE END AS is_current
    FROM source
)

SELECT * FROM renamed