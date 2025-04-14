-- Customer cross-sell eligibility: Determine which products customers are eligible for based on their profile

WITH customer_tenure AS (
    SELECT 
        customer_id,
        customer_tenure_segment,
        total_months_as_customer,
        current_plan_type,
        current_plan_tier,
        current_tier_level,
        active_percentage
    FROM {{ ref('int_customer__tenure') }}
),

marketplace_activity AS (
    SELECT
        customer_id,
        SUM(marketplace_searches) AS total_marketplace_searches,
        SUM(marketplace_leads) AS total_marketplace_leads,
        SUM(marketplace_revenue) AS total_marketplace_revenue,
        MAX(CASE WHEN marketplace_searches_creditcard > 0 THEN TRUE ELSE FALSE END) AS has_searched_creditcard,
        MAX(CASE WHEN marketplace_searches_loan > 0 THEN TRUE ELSE FALSE END) AS has_searched_loan,
        MAX(CASE WHEN marketplace_searches_car_finance > 0 THEN TRUE ELSE FALSE END) AS has_searched_car_finance,
        MAX(CASE WHEN marketplace_searches_car_re_finance > 0 THEN TRUE ELSE FALSE END) AS has_searched_car_re_finance
    FROM {{ ref('mart_marketplace__customer_kpis') }}
    GROUP BY customer_id
)

SELECT 
    ct.customer_id,
    ct.customer_tenure_segment,
    ct.total_months_as_customer,
    ct.current_plan_type,
    ct.current_plan_tier,
    ct.current_tier_level,
    ct.active_percentage,
    
    -- Marketplace activity summary
    COALESCE(ma.total_marketplace_searches, 0) AS total_marketplace_searches,
    COALESCE(ma.total_marketplace_leads, 0) AS total_marketplace_leads,
    COALESCE(ma.total_marketplace_revenue, 0) AS total_marketplace_revenue,
    COALESCE(ma.has_searched_creditcard, FALSE) AS has_searched_creditcard,
    COALESCE(ma.has_searched_loan, FALSE) AS has_searched_loan,
    COALESCE(ma.has_searched_car_finance, FALSE) AS has_searched_car_finance,
    COALESCE(ma.has_searched_car_re_finance, FALSE) AS has_searched_car_re_finance,
    
    -- Cross-sell eligibility flags based on customer profile
    CASE 
        WHEN ct.current_plan_type = 'Basic' AND ct.total_months_as_customer >= 3 AND ct.active_percentage > 80 THEN TRUE
        ELSE FALSE
    END AS eligible_for_premium_upgrade,
    
    CASE 
        WHEN ct.current_tier_level < 3 AND ct.total_months_as_customer >= 6 AND ct.active_percentage > 90 THEN TRUE
        ELSE FALSE
    END AS eligible_for_tier_upgrade,
    
    CASE 
        WHEN ct.current_plan_type = 'Premium' AND ct.customer_tenure_segment IN ('Loyal', 'Engaged') 
             AND COALESCE(ma.total_marketplace_searches, 0) > 0 THEN TRUE
        ELSE FALSE
    END AS eligible_for_exclusive_offers,
    
    CASE 
        WHEN COALESCE(ma.has_searched_creditcard, FALSE) = TRUE AND ct.customer_tenure_segment IN ('Loyal', 'Engaged') THEN TRUE
        ELSE FALSE
    END AS eligible_for_premium_creditcard,
    
    CASE 
        WHEN COALESCE(ma.has_searched_loan, FALSE) = TRUE AND ct.customer_tenure_segment IN ('Loyal', 'Engaged') THEN TRUE
        ELSE FALSE
    END AS eligible_for_preferred_loan_rates,
    
    -- Overall cross-sell score (0-100)
    (CASE WHEN ct.customer_tenure_segment = 'Loyal' THEN 30
          WHEN ct.customer_tenure_segment = 'Engaged' THEN 20
          WHEN ct.customer_tenure_segment = 'Recent' THEN 10
          ELSE 5
     END) +
    (CASE WHEN ct.current_plan_type = 'Premium' THEN 20 ELSE 10 END) +
    (CASE WHEN ct.current_tier_level >= 2 THEN 15 ELSE 5 END) +
    (CASE WHEN ct.active_percentage > 90 THEN 15 ELSE 5 END) +
    (CASE WHEN COALESCE(ma.total_marketplace_leads, 0) > 0 THEN 20 ELSE 0 END)
    AS cross_sell_score
FROM customer_tenure ct
LEFT JOIN marketplace_activity ma
    ON ct.customer_id = ma.customer_id