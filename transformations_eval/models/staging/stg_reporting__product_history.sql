WITH source AS (
    SELECT * FROM {{ source('reporting', 'producthistory') }}
)

SELECT
    APPLICANTID AS customer_id,
    CREATEDT::DATE AS report_date,
    EXPERIANRESULTSETID AS experian_resultset_id,
    LOWER(REQUESTEDPRODUCT) AS requested_product
FROM source