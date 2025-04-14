WITH source AS (
    SELECT * FROM {{ source('raw', 'subscriptions') }}
),

renamed AS (
    SELECT
        subscription_id,
        customer_id,
        product_id,
        start_date,
        end_date,
        status,
        payment_method,
        billing_frequency,
        CAST(monthly_amount AS DECIMAL(10,2)) AS monthly_amount
    FROM source
)

SELECT * FROM renamed