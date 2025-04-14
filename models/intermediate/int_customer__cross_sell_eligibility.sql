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

-- First, let's get the column names from the mart_marketplace__customer_kpis table
-- to ensure we're using the right references
marketplace_activity AS (
    SELECT
        customer_id,
        SUM(marketplace_searches) AS total_marketplace_searches,
        SUM(marketplace_leads) AS total_marketplace_leads,
        SUM(marketplace_revenue) AS total_marketplace_revenue,
        -- Instead of looking for specific product searches, we'll use the total search count
        -- (If you do have these columns, you can uncomment the specific product lines)
        COUNT(DISTINCT report_date) AS active_days
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
    COALESCE(ma.active_days, 0) AS active_days,
    
    -- We don't have product-specific search data, so we'll set these to FALSE by default
    -- In a real implementation, you'd want to get this data from the appropriate source
    FALSE AS has_searched_creditcard,
    FALSE AS has_searched_loan,
    FALSE AS has_searched_car_finance,
    FALSE AS has_searched_car_re_finance,
    
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
    
    -- Since we don't have product-specific search data, we'll base eligibility on overall activity
    CASE 
        WHEN COALESCE(ma.total_marketplace_searches, 0) > 0 AND ct.customer_tenure_segment IN ('Loyal', 'Engaged') THEN TRUE
        ELSE FALSE
    END AS eligible_for_premium_creditcard,
    
    CASE 
        WHEN COALESCE(ma.total_marketplace_searches, 0) > 0 AND ct.customer_tenure_segment IN ('Loyal', 'Engaged') THEN TRUE
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