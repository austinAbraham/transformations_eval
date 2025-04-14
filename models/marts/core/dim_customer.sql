WITH customer_latest AS (
    SELECT
        c.customer_id,
        c.customer_age,
        c.customer_age_band,
        c.customer_is_prospect,
        a.acquisition_date,
        a.acquisition_type,
        a.acquisition_channel,
        MAX(CASE WHEN s.status = 'active' THEN s.product_id ELSE NULL END) AS current_product_id,
        MAX(CASE WHEN s.status = 'active' THEN p.product_family ELSE NULL END) AS current_product_family,
        MIN(s.start_date) AS first_subscription_date,
        COUNT(DISTINCT s.subscription_id) AS total_subscriptions,
        SUM(CASE WHEN s.status = 'active' THEN s.monthly_amount ELSE 0 END) AS current_monthly_revenue
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('int_customer_acquisition_source') }} a
        ON c.customer_id = a.customer_id
    LEFT JOIN {{ ref('stg_subscriptions') }} s
        ON c.customer_id = s.customer_id
    LEFT JOIN {{ ref('stg_products') }} p
        ON s.product_id = p.product_id
    WHERE c.is_current = TRUE
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT * FROM customer_latest