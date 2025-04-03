WITH source AS (
    SELECT * FROM {{ source('reporting', 'leads') }}
)

SELECT
    APPLICANTID AS customer_id,
    CREATEDT::DATE AS report_date,
    LEADID AS lead_id,
    EXPERIANRESULTSETID AS experian_resultset_id,
    {{ get_lead_type('PRODUCTPROVIDER') }} AS lead_type,
    PRODUCTID || PRODUCTCATEGORY || PRODUCTDESCRIPTION AS rpl_id,
    PRODUCTPROVIDER AS product_provider,
    PRODUCTID AS product_id,
    PRODUCTCATEGORY AS product_category,
    PRODUCTDESCRIPTION AS product_description
FROM source