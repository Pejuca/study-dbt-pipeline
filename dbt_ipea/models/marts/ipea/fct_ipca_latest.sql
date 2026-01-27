wITH base AS (
    SELECT * FROM {{
        ref('fct_ipca_monthly')
    }}
)

SELECT * 
FROM base 
QUALIFY ref_date = MAX(
    ref_date 
) OVER ()