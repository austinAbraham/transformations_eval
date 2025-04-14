-- Customer 360 model: Comprehensive view of customer combining all data sources

WITH customer_tenure AS (
    SELECT 
        customer_id,
        first_subscription_date,
        total_days_as_customer,
        total_months_as_customer,
        customer_tenure_segment,
        current_subscription_id,
        current_plan_type,
        current_plan_tier,
        current_tier_level,
        current_monthly_fee,
        active_percentage
    FROM {{ ref('int_customer__tenure') }}
),

customer_referrals AS (
    SELECT
        customer_id,
        total_referrals_made,
        successful_referrals,
        total_referral_bonus_earned,
        first_referral_date,
        last_referral_date,
        referrals_last_3_months,
        is_referred_customer,
        date_referred,
        referred_by_customer_id,
        referrer_segment,
        is_active_referrer
    FROM {{ ref('int_customer__referral_metrics') }}
),

customer_cross_sell AS (
    SELECT
        customer_id,
        eligible_for_premium_upgrade,
        eligible_for_tier_upgrade,
        eligible_for_exclusive_offers,
        eligible_for_premium_creditcard,
        eligible_for_preferred_loan_rates,
        cross_sell_score
    FROM {{ ref('int_customer__cross_sell_eligibility') }}
),

marketplace_activity AS (
    SELECT
        customer_id,
        SUM(marketplace_searches) AS total_searches,
        SUM(marketplace_leads) AS total_leads,
        SUM(marketplace_revenue) AS total_revenue,
        MAX(report_date) AS last_activity_date,
        COUNT(DISTINCT report_date) AS active_days
        -- We're no longer referencing product-specific metrics that don't exist
    FROM {{ ref('mart_marketplace__customer_kpis') }}
    GROUP BY customer_id
)

SELECT
    -- Customer identifier
    COALESCE(ct.customer_id, 
             cr.customer_id, 
             cs.customer_id, 
             ma.customer_id) AS customer_id,
             
    -- Tenure information
    ct.first_subscription_date,
    ct.total_days_as_customer,
    ct.total_months_as_customer,
    ct.customer_tenure_segment,
    
    -- Current subscription information
    ct.current_subscription_id,
    ct.current_plan_type,
    ct.current_plan_tier,
    ct.current_tier_level,
    ct.current_monthly_fee,
    ct.active_percentage,
    
    -- Referral activity
    COALESCE(cr.total_referrals_made, 0) AS total_referrals_made,
    COALESCE(cr.successful_referrals, 0) AS successful_referrals,
    COALESCE(cr.total_referral_bonus_earned, 0) AS total_referral_bonus_earned,
    cr.first_referral_date,
    cr.last_referral_date,
    COALESCE(cr.referrals_last_3_months, 0) AS referrals_last_3_months,
    COALESCE(cr.is_referred_customer, FALSE) AS is_referred_customer,
    cr.date_referred,
    cr.referred_by_customer_id,
    cr.referrer_segment,
    COALESCE(cr.is_active_referrer, FALSE) AS is_active_referrer,
    
    -- Marketplace activity
    COALESCE(ma.total_searches, 0) AS marketplace_total_searches,
    COALESCE(ma.total_leads, 0) AS marketplace_total_leads,
    COALESCE(ma.total_revenue, 0) AS marketplace_total_revenue,
    ma.last_activity_date AS marketplace_last_activity_date,
    COALESCE(ma.active_days, 0) AS marketplace_active_days,
    
    -- We don't have product-specific metrics, so we'll set them to 0
    0 AS marketplace_creditcard_searches,
    0 AS marketplace_loan_searches,
    0 AS marketplace_car_finance_searches,
    0 AS marketplace_car_refinance_searches,
    
    -- Other marketplace metrics
    0 AS marketplace_regular_leads,
    0 AS marketplace_zero_elig_leads,
    0 AS marketplace_regular_revenue,
    0 AS marketplace_zero_elig_revenue,
    
    -- Cross-sell eligibility
    COALESCE(cs.eligible_for_premium_upgrade, FALSE) AS eligible_for_premium_upgrade,
    COALESCE(cs.eligible_for_tier_upgrade, FALSE) AS eligible_for_tier_upgrade,
    COALESCE(cs.eligible_for_exclusive_offers, FALSE) AS eligible_for_exclusive_offers,
    COALESCE(cs.eligible_for_premium_creditcard, FALSE) AS eligible_for_premium_creditcard,
    COALESCE(cs.eligible_for_preferred_loan_rates, FALSE) AS eligible_for_preferred_loan_rates,
    COALESCE(cs.cross_sell_score, 0) AS cross_sell_score,
    
    -- Derived high-level metrics
    CASE 
        WHEN ct.customer_tenure_segment = 'Loyal' AND cr.referrer_segment IN ('Advocate', 'Champion') 
             AND ma.total_revenue > 100 THEN 'VIP'
        WHEN ct.customer_tenure_segment = 'Loyal' OR cr.referrer_segment = 'Champion' 
             OR ma.total_revenue > 100 THEN 'High-Value'
        WHEN ct.customer_tenure_segment = 'Engaged' OR cr.total_referrals_made > 0 
             OR ma.total_leads > 0 THEN 'Mid-Value'
        ELSE 'Standard'
    END AS customer_value_segment,
    
    CASE 
        WHEN (COALESCE(ma.total_searches, 0) > 0 OR ct.current_subscription_id IS NOT NULL)
             AND COALESCE(ma.last_activity_date, CURRENT_DATE()) >= DATEADD(month, -3, CURRENT_DATE()) THEN 'Active'
        WHEN (COALESCE(ma.total_searches, 0) > 0 OR ct.current_subscription_id IS NOT NULL)
             AND COALESCE(ma.last_activity_date, CURRENT_DATE()) >= DATEADD(month, -6, CURRENT_DATE()) THEN 'Recent'
        WHEN (COALESCE(ma.total_searches, 0) > 0 OR ct.current_subscription_id IS NOT NULL) THEN 'Lapsed'
        ELSE 'Inactive'
    END AS activity_status,
    
    CURRENT_TIMESTAMP() AS updated_at
FROM customer_tenure ct
FULL OUTER JOIN customer_referrals cr
    ON ct.customer_id = cr.customer_id
FULL OUTER JOIN customer_cross_sell cs
    ON ct.customer_id = cs.customer_id
FULL OUTER JOIN marketplace_activity ma
    ON ct.customer_id = ma.customer_id