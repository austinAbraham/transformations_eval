-- Customer tenure model: Calculate current and historical tenure metrics for each customer

WITH customer_subscriptions AS (
    SELECT 
        customer_id,
        subscription_id,
        plan_type,
        plan_tier,
        start_date,
        end_date,
        status,
        monthly_fee,
        tier_level,
        is_active
    FROM {{ ref('stg_customer__subscriptions') }}
),

-- Get the first subscription date for each customer (acquisition date)
first_subscription AS (
    SELECT
        customer_id,
        MIN(start_date) AS first_subscription_date
    FROM customer_subscriptions
    GROUP BY customer_id
),

-- Calculate the tenure for each active subscription
active_subscriptions AS (
    SELECT
        cs.customer_id,
        cs.subscription_id,
        cs.plan_type,
        cs.plan_tier,
        cs.tier_level,
        cs.start_date,
        cs.end_date,
        cs.monthly_fee,
        cs.status,
        DATEDIFF(day, cs.start_date, CURRENT_DATE()) AS days_subscribed,
        DATEDIFF(month, cs.start_date, CURRENT_DATE()) AS months_subscribed
    FROM customer_subscriptions cs
    WHERE cs.is_active = TRUE
),

-- Calculate total tenure (including historical subscriptions)
customer_tenure AS (
    SELECT
        cs.customer_id,
        fs.first_subscription_date,
        DATEDIFF(day, fs.first_subscription_date, CURRENT_DATE()) AS total_days_as_customer,
        DATEDIFF(month, fs.first_subscription_date, CURRENT_DATE()) AS total_months_as_customer,
        SUM(DATEDIFF(day, cs.start_date, 
                    CASE 
                        WHEN cs.status = 'Active' THEN CURRENT_DATE()
                        ELSE LEAST(cs.end_date, CURRENT_DATE())
                    END)) AS total_active_days,
        COUNT(DISTINCT cs.subscription_id) AS total_subscriptions,
        SUM(CASE WHEN cs.status = 'Canceled' THEN 1 ELSE 0 END) AS total_cancellations
    FROM customer_subscriptions cs
    JOIN first_subscription fs
        ON cs.customer_id = fs.customer_id
    GROUP BY cs.customer_id, fs.first_subscription_date
)

SELECT 
    ct.customer_id,
    ct.first_subscription_date,
    ct.total_days_as_customer,
    ct.total_months_as_customer,
    ct.total_active_days,
    ct.total_subscriptions,
    ct.total_cancellations,
    CASE 
        WHEN ct.total_active_days > 365 THEN 'Loyal'
        WHEN ct.total_active_days > 180 THEN 'Engaged'
        WHEN ct.total_active_days > 90 THEN 'Recent'
        ELSE 'New'
    END AS customer_tenure_segment,
    
    -- Include active subscription details if applicable
    MAX(CASE WHEN as2.subscription_id IS NOT NULL THEN as2.subscription_id ELSE NULL END) AS current_subscription_id,
    MAX(CASE WHEN as2.subscription_id IS NOT NULL THEN as2.plan_type ELSE NULL END) AS current_plan_type,
    MAX(CASE WHEN as2.subscription_id IS NOT NULL THEN as2.plan_tier ELSE NULL END) AS current_plan_tier,
    MAX(CASE WHEN as2.subscription_id IS NOT NULL THEN as2.tier_level ELSE NULL END) AS current_tier_level,
    MAX(CASE WHEN as2.subscription_id IS NOT NULL THEN as2.monthly_fee ELSE NULL END) AS current_monthly_fee,
    MAX(CASE WHEN as2.subscription_id IS NOT NULL THEN as2.days_subscribed ELSE NULL END) AS current_days_subscribed,
    
    -- Calculated loyalty metrics
    CASE 
        WHEN ct.total_active_days > 0 THEN 
            ROUND(100.0 * ct.total_active_days / NULLIF(ct.total_days_as_customer, 0), 2)
        ELSE 0
    END AS active_percentage
FROM customer_tenure ct
LEFT JOIN active_subscriptions as2
    ON ct.customer_id = as2.customer_id
GROUP BY 
    ct.customer_id,
    ct.first_subscription_date,
    ct.total_days_as_customer,
    ct.total_months_as_customer,
    ct.total_active_days,
    ct.total_subscriptions,
    ct.total_cancellations