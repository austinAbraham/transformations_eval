WITH source AS (
    SELECT * FROM {{ source('analytics_corvette', 'marketplace_trading_rpls_combined') }}
)

SELECT
    ID AS rpl_id,
    MONTH_ AS month,
    RPL_FORECAST AS rpl_forecast
FROM source