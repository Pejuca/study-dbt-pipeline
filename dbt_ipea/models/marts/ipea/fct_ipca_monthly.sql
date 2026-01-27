SELECT
    ref_date,
    serie_code_filled AS serie_code,
    ipca_nivel_filled AS ipca_nivel,
    ipca_mom,
    ipca_yoy
FROM {{
    ref('int_ipca_metrics')
}}
WHERE ipca_nivel_filled IS NOT NULL