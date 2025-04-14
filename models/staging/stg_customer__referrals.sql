WITH source AS (
    SELECT * FROM {{ source('customer', 'referrals') }}
)

SELECT
    REFERRAL_ID as referral_id,
    REFERRER_CUSTOMER_ID as referrer_customer_id,
    REFERRED_CUSTOMER_ID as referred_customer_id,
    REFERRAL_DATE as referral_date,
    REFERRAL_STATUS as referral_status,
    BONUS_AMOUNT as bonus_amount,
    CASE 
        WHEN REFERRAL_STATUS = 'Converted' THEN TRUE 
        ELSE FALSE 
    END as is_converted
FROM source