-- Customer retention model: Focus on customer retention metrics and churn risk

WITH customer_history AS (
    SELECT 
        customer_id,
        subscription_id,
        plan_type,
        plan_tier,
        start_date,
        end_date,
        status,
        monthly_fee,
        CASE WHEN status = 'Canceled' THEN TRUE ELSE FALSE END AS is_churned
    FROM {{ ref('stg_customer__subscriptions') }}
),

-- Get the most recent subscription for each customer
latest_subscription AS (
    SELECT 
        customer_id,
        subscription_id,
        plan_type,
        plan_tier,
        start_date,
        end_date,
        status,
        monthly_fee,
        is_churned,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS subscription_rank
    FROM customer_history
),

customer_tenure AS (
    SELECT 
        customer_id,
        first_subscription_date,
        customer_tenure_segment,
        total_months_as_customer,
        active_percentage
    FROM {{ ref('int_customer__tenure') }}
),

customer_activity AS (
    SELECT
        customer_id,
        marketplace_total_searches,
        marketplace_total_leads,
        marketplace_last_activity_date,
        marketplace_active_days,
        activity_status
    FROM {{ ref('mart_customer_360') }}
),

-- Calculate churn metrics
churn_data AS (
    SELECT
        ch.customer_id,
        COUNT(DISTINCT ch.subscription_id) AS total_subscriptions,
        SUM(CASE WHEN ch.is_churned THEN 1 ELSE 0 END) AS number_of_churns,
        MAX(CASE WHEN ch.is_churned THEN ch.end_date ELSE NULL END) AS last_churn_date,
        -- Look at time between churns if multiple churns
        AVG(CASE 
                WHEN ch.is_churned AND LAG(ch.is_churned) OVER (PARTITION BY ch.customer_id ORDER BY ch.start_date) = TRUE 
                THEN DATEDIFF(day, LAG(ch.end_date) OVER (PARTITION BY ch.customer_id ORDER BY ch.start_date), ch.end_date)
                ELSE NULL
            END) AS avg_days_between_churns
    FROM customer_history ch
    GROUP BY ch.customer_id
)

SELECT
    -- Customer identification
    ls.customer_id,
    
    -- Current subscription status
    ls.subscription_id AS latest_subscription_id,
    ls.plan_type AS latest_plan_type,
    ls.plan_tier AS latest_plan_tier,
    ls.start_date AS latest_subscription_start,
    ls.end_date AS latest_subscription_end,
    ls.status AS latest_subscription_status,
    ls.monthly_fee AS latest_monthly_fee,
    
    -- Tenure information
    ct.first_subscription_date,
    ct.customer_tenure_segment,
    ct.total_months_as_customer,
    
    -- Churn history
    cd.total_subscriptions,
    cd.number_of_churns,
    cd.last_churn_date,
    cd.avg_days_between_churns,
    
    -- Activity metrics
    ca.marketplace_total_searches,
    ca.marketplace_total_leads,
    ca.marketplace_last_activity_date,
    ca.marketplace_active_days,
    ca.activity_status,
    
    -- Retention metrics
    ct.active_percentage,
    
    -- Calculate time since last activity
    DATEDIFF(day, COALESCE(ca.marketplace_last_activity_date, ls.start_date), CURRENT_DATE()) AS days_since_last_activity,
    
    -- Calculate renewal metrics for active subscriptions
    CASE 
        WHEN ls.status = 'Active' THEN DATEDIFF(day, CURRENT_DATE(), ls.end_date)
        ELSE NULL
    END AS days_until_renewal,
    
    -- Churn risk scoring (0-100)
    CASE WHEN ls.status = 'Canceled' THEN 100 -- Already churned
        ELSE
            -- Base score depending on tenure
            (CASE 
                WHEN ct.customer_tenure_segment = 'New' THEN 40
                WHEN ct.customer_tenure_segment = 'Recent' THEN 30
                WHEN ct.customer_tenure_segment = 'Engaged' THEN 20
                WHEN ct.customer_tenure_segment = 'Loyal' THEN 10
                ELSE 35
             END) +
            
            -- Score based on activity
            (CASE
                WHEN ca.activity_status = 'Active' THEN 0
                WHEN ca.activity_status = 'Recent' THEN 15
                WHEN ca.activity_status = 'Lapsed' THEN 30
                WHEN ca.activity_status = 'Inactive' THEN 45
                ELSE 25
             END) +
            
            -- Score based on previous churn behavior
            (CASE
                WHEN cd.number_of_churns > 1 THEN 20
                WHEN cd.number_of_churns = 1 THEN 10
                ELSE 0
             END) +
            
            -- Adjustment for marketplace engagement
            (CASE
                WHEN ca.marketplace_total_leads > 5 THEN -10
                WHEN ca.marketplace_total_searches > 10 THEN -5
                ELSE 0
             END)
    END AS churn_risk_score,
    
    -- Churn risk category
    CASE 
        WHEN ls.status = 'Canceled' THEN 'Churned'
        WHEN ls.status = 'Active' AND 
            (CASE 
                WHEN ct.customer_tenure_segment = 'New' THEN 40
                WHEN ct.customer_tenure_segment = 'Recent' THEN 30
                WHEN ct.customer_tenure_segment = 'Engaged' THEN 20
                WHEN ct.customer_tenure_segment = 'Loyal' THEN 10
                ELSE 35
             END) +
            (CASE
                WHEN ca.activity_status = 'Active' THEN 0
                WHEN ca.activity_status = 'Recent' THEN 15
                WHEN ca.activity_status = 'Lapsed' THEN 30
                WHEN ca.activity_status = 'Inactive' THEN 45
                ELSE 25
             END) +
            (CASE
                WHEN cd.number_of_churns > 1 THEN 20
                WHEN cd.number_of_churns = 1 THEN 10
                ELSE 0
             END) +
            (CASE
                WHEN ca.marketplace_total_leads > 5 THEN -10
                WHEN ca.marketplace_total_searches > 10 THEN -5
                ELSE 0
             END) >= 70 THEN 'High Risk'
        WHEN ls.status = 'Active' AND 
            (CASE 
                WHEN ct.customer_tenure_segment = 'New' THEN 40
                WHEN ct.customer_tenure_segment = 'Recent' THEN 30
                WHEN ct.customer_tenure_segment = 'Engaged' THEN 20
                WHEN ct.customer_tenure_segment = 'Loyal' THEN 10
                ELSE 35
             END) +
            (CASE
                WHEN ca.activity_status = 'Active' THEN 0
                WHEN ca.activity_status = 'Recent' THEN 15
                WHEN ca.activity_status = 'Lapsed' THEN 30
                WHEN ca.activity_status = 'Inactive' THEN 45
                ELSE 25
             END) +
            (CASE
                WHEN cd.number_of_churns > 1 THEN 20
                WHEN cd.number_of_churns = 1 THEN 10
                ELSE 0
             END) +
            (CASE
                WHEN ca.marketplace_total_leads > 5 THEN -10
                WHEN ca.marketplace_total_searches > 10 THEN -5
                ELSE 0
             END) >= 40 THEN 'Medium Risk'
        WHEN ls.status = 'Active' THEN 'Low Risk'
        ELSE 'Unknown'
    END AS churn_risk_category,
    
    -- Recommended retention actions
    CASE 
        WHEN ls.status = 'Canceled' THEN 'Reactivation campaign'
        WHEN ls.status = 'Active' AND 
            (CASE 
                WHEN ct.customer_tenure_segment = 'New' THEN 40
                WHEN ct.customer_tenure_segment = 'Recent' THEN 30
                WHEN ct.customer_tenure_segment = 'Engaged' THEN 20
                WHEN ct.customer_tenure_segment = 'Loyal' THEN 10
                ELSE 35
             END) +
            (CASE
                WHEN ca.activity_status = 'Active' THEN 0
                WHEN ca.activity_status = 'Recent' THEN 15
                WHEN ca.activity_status = 'Lapsed' THEN 30
                WHEN ca.activity_status = 'Inactive' THEN 45
                ELSE 25
             END) +
            (CASE
                WHEN cd.number_of_churns > 1 THEN 20
                WHEN cd.number_of_churns = 1 THEN 10
                ELSE 0
             END) +
            (CASE
                WHEN ca.marketplace_total_leads > 5 THEN -10
                WHEN ca.marketplace_total_searches > 10 THEN -5
                ELSE 0
             END) >= 70 THEN 'Immediate outreach with special offer'
        WHEN ls.status = 'Active' AND 
            (CASE 
                WHEN ct.customer_tenure_segment = 'New' THEN 40
                WHEN ct.customer_tenure_segment = 'Recent' THEN 30
                WHEN ct.customer_tenure_segment = 'Engaged' THEN 20
                WHEN ct.customer_tenure_segment = 'Loyal' THEN 10
                ELSE 35
             END) +
            (CASE
                WHEN ca.activity_status = 'Active' THEN 0
                WHEN ca.activity_status = 'Recent' THEN 15
                WHEN ca.activity_status = 'Lapsed' THEN 30
                WHEN ca.activity_status = 'Inactive' THEN 45
                ELSE 25
             END) +
            (CASE
                WHEN cd.number_of_churns > 1 THEN 20
                WHEN cd.number_of_churns = 1 THEN 10
                ELSE 0
             END) +
            (CASE
                WHEN ca.marketplace_total_leads > 5 THEN -10
                WHEN ca.marketplace_total_searches > 10 THEN -5
                ELSE 0
             END) >= 40 THEN 'Engagement campaign'
        WHEN ls.status = 'Active' THEN 'Regular nurture'
        ELSE 'Needs review'
    END AS recommended_retention_action,
    
    CURRENT_TIMESTAMP() AS updated_at
FROM latest_subscription ls
LEFT JOIN customer_tenure ct
    ON ls.customer_id = ct.customer_id
LEFT JOIN churn_data cd
    ON ls.customer_id = cd.customer_id
LEFT JOIN customer_activity ca
    ON ls.customer_id = ca.customer_id
WHERE ls.subscription_rank = 1