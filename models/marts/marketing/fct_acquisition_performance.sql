WITH acquisition_metrics AS (
    SELECT
        DATE_TRUNC('day', a.acquisition_date) AS acquisition_date,
        a.acquisition_channel,
        a.acquisition_type,
        COUNT(DISTINCT a.customer_id) AS new_customers,
        COUNT(DISTINCT CASE WHEN c.customer_is_prospect = FALSE THEN a.customer_id ELSE NULL END) AS converted_customers,
        AVG(EXTRACT(YEAR FROM a.acquisition_date) - c.customer_age) AS avg_birth_year,
        MODE() WITHIN GROUP (ORDER BY c.customer_age_band) AS most_common_age_band
    FROM {{ ref('int_customer_acquisition_source') }} a
    JOIN {{ ref('stg_customers') }} c
        ON a.customer_id = c.customer_id
    GROUP BY 1, 2, 3
)

SELECT
    acquisition_metrics.*,
    CASE WHEN new_customers > 0 
         THEN ROUND(converted_customers::numeric / new_customers, 4) 
         ELSE 0 
    END AS conversion_rate
FROM acquisition_metrics