{% macro get_lead_type(product_provider) %}
    CASE LOWER({{ product_provider }})
        WHEN 'zilch' THEN 0
        WHEN 'zilch up' THEN 0
        WHEN 'zilch classic' THEN 0
        WHEN 'credit spring' THEN 0
        WHEN 'creditspring' THEN 0
        WHEN 'aspiremoney' THEN 0 
        WHEN 'buildmycreditscore' THEN 0 
        WHEN 'drafty' THEN 0
        ELSE 1
    END
{% endmacro %}

{% macro get_report_date() %}
    {% if var('report_date', none) is none %}
        CURRENT_DATE()
    {% else %}
        DATE('{{ var('report_date') }}')
    {% endif %}
{% endmacro %}