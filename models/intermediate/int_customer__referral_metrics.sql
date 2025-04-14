-- Customer referral metrics: Calculate referral performance for each customer

WITH referrals AS (
    SELECT
        referral_id,
        referrer_customer_id,
        referred_customer_id,
        referral_date,
        referral_status,
        bonus_amount,
        is_converted
    FROM {{ ref('stg_customer__referrals') }}
),

-- Referrer metrics (customers who have referred others)
referrer_metrics AS (
    SELECT
        referrer_customer_id AS customer_id,
        COUNT(referral_id) AS total_referrals_made,
        SUM(CASE WHEN is_converted THEN 1 ELSE 0 END) AS successful_referrals,
        SUM(bonus_amount) AS total_referral_bonus_earned,
        MIN(referral_date) AS first_referral_date,
        MAX(referral_date) AS last_referral_date,
        SUM(CASE WHEN referral_date >= DATEADD(month, -3, CURRENT_DATE()) THEN 1 ELSE 0 END) AS referrals_last_3_months
    FROM referrals
    GROUP BY referrer_customer_id
),

-- Referred metrics (customers who were referred by others)
referred_metrics AS (
    SELECT
        referred_customer_id AS customer_id,
        MIN(referral_date) AS date_referred,
        MAX(referrer_customer_id) AS referred_by_customer_id
    FROM referrals
    WHERE is_converted = TRUE
    GROUP BY referred_customer_id
)

-- Combine metrics for a complete view
SELECT 
    COALESCE(rm.customer_id, rfm.customer_id) AS customer_id,
    
    -- Referrer metrics
    COALESCE(rm.total_referrals_made, 0) AS total_referrals_made,
    COALESCE(rm.successful_referrals, 0) AS successful_referrals,
    COALESCE(rm.total_referral_bonus_earned, 0) AS total_referral_bonus_earned,
    rm.first_referral_date,
    rm.last_referral_date,
    COALESCE(rm.referrals_last_3_months, 0) AS referrals_last_3_months,
    
    -- Referred metrics
    CASE WHEN rfm.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_referred_customer,
    rfm.date_referred,
    rfm.referred_by_customer_id,
    
    -- Derived metrics
    CASE 
        WHEN COALESCE(rm.successful_referrals, 0) >= 5 THEN 'Champion'
        WHEN COALESCE(rm.successful_referrals, 0) >= 3 THEN 'Advocate'
        WHEN COALESCE(rm.successful_referrals, 0) >= 1 THEN 'Referrer'
        ELSE 'Non-referrer'
    END AS referrer_segment,
    
    CASE 
        WHEN COALESCE(rm.referrals_last_3_months, 0) > 0 THEN TRUE 
        ELSE FALSE 
    END AS is_active_referrer,
    
    CASE 
        WHEN COALESCE(rm.total_referrals_made, 0) > 0 THEN
            ROUND(COALESCE(rm.successful_referrals, 0) * 100.0 / NULLIF(rm.total_referrals_made, 0), 2)
        ELSE 0
    END AS referral_conversion_rate
FROM referrer_metrics AS rm
FULL OUTER JOIN referred_metrics AS rfm
    ON rm.customer_id = rfm.customer_id