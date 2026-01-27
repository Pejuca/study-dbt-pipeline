SELECT
    ref_date,
    ipca_yoy
FROM {{
    ref('fct_ipca_monthly')
}}
WHERE ipca_yoy IS NOT NULL