WITH searches AS (
    SELECT * FROM {{ ref('int_marketplace__searches') }}
),

leads AS (
    SELECT * FROM {{ ref('int_marketplace__leads_classified') }}
)

SELECT
    s.customer_id,
    s.report_date,
    s.experian_resultset_id,
    s.requested_product,
    l.lead_id,
    l.lead_type,
    l.rpl_id
FROM searches s
LEFT JOIN leads l
    ON s.experian_resultset_id = l.experian_resultset_id