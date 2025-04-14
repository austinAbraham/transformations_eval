WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

renamed AS (
    SELECT
        product_id,
        product_name,
        product_family,
        product_type,
        CAST(price AS DECIMAL(10,2)) AS price,
        is_active
    FROM source
)

SELECT * FROM renamed