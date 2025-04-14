WITH source AS (
    SELECT * FROM {{ source('customer', 'subscriptions') }}
)

SELECT
    CUSTOMER_ID as customer_id,
    SUBSCRIPTION_ID as subscription_id,
    PLAN_TYPE as plan_type,
    PLAN_TIER as plan_tier,
    START_DATE as start_date,
    END_DATE as end_date,
    STATUS as status,
    MONTHLY_FEE as monthly_fee,
    PAYMENT_METHOD as payment_method,
    CASE 
        WHEN PLAN_TYPE = 'Premium' AND PLAN_TIER = 'Platinum' THEN 3
        WHEN PLAN_TYPE = 'Premium' AND PLAN_TIER = 'Gold' THEN 2
        WHEN PLAN_TYPE = 'Basic' AND PLAN_TIER = 'Silver' THEN 1
        WHEN PLAN_TYPE = 'Basic' AND PLAN_TIER = 'Bronze' THEN 0
        ELSE -1
    END as tier_level,
    CASE 
        WHEN STATUS = 'Active' THEN TRUE 
        ELSE FALSE 
    END as is_active
FROM source