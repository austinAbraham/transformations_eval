WITH searches_leads AS (
    SELECT * FROM {{ ref('int_marketplace__searches_with_leads') }}
),

rpls AS (
    SELECT * FROM {{ ref('stg_analytics_corvette__marketplace_trading_rpls') }}
),

searches_leads_revenue AS (
    SELECT
        sl.customer_id,
        sl.report_date,
        sl.experian_resultset_id,
        sl.requested_product,
        sl.lead_id,
        sl.lead_type,
        sl.rpl_id,
        NVL(r.rpl_forecast, 0) AS revenue
    FROM searches_leads sl
    LEFT JOIN rpls r
        ON sl.rpl_id = r.rpl_id
),

rpl_row_num AS (
    -- Make sure there is only 1 RPL per lead in case there are duplicate RPLs
    SELECT
        customer_id,
        report_date,
        experian_resultset_id,
        requested_product,
        lead_id,
        lead_type,
        rpl_id,
        revenue,
        ROW_NUMBER() OVER(PARTITION BY NVL(lead_id, experian_resultset_id) ORDER BY revenue DESC) AS rpl_rn
        -- Added ORDER BY revenue DESC to sort by highest revenue first if there are duplicates
    FROM searches_leads_revenue
)

SELECT
    customer_id,
    report_date,
    experian_resultset_id,
    requested_product,
    lead_id,
    lead_type,
    rpl_id,
    revenue
FROM rpl_row_num
WHERE NVL(rpl_rn, 1) = 1