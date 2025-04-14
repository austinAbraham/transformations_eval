WITH customer_subscriptions AS (
    SELECT
        c.customer_id,
        c.customer_age,
        c.customer_age_band,
        s.product_id,
        p.product_family,
        p.product_type,
        s.start_date,
        s.end_date,
        s.status,
        s.monthly_amount,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY s.start_date) AS subscription_sequence
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_subscriptions') }} s
        ON c.customer_id = s.customer_id
    LEFT JOIN {{ ref('stg_products') }} p
        ON s.product_id = p.product_id
)

SELECT * FROM customer_subscriptions