WITH base AS (
    SELECT
        ref_date,
        serie_code,
        ipca_nivel
    FROM {{
        ref('stg_ipca')
    }}
),

-- ffilling base

fill_b as (
    SELECT
        ref_date,
        
        LAST_VALUE(serie_code IGNORE NULLS)
        OVER (
            ORDER BY ref_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS serie_code_filled,

        LAST_VALUE(ipca_nivel IGNORE NULLS)
        OVER (
            ORDER BY ref_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ipca_nivel_filled

FROM base),

-- Métricas

ipca_metrics AS (
    SELECT 
        ref_date,
        serie_code_filled,
        ipca_nivel_filled,

        -- MoM
        100.0 * (ipca_nivel_filled/ LAG(ipca_nivel_filled, 1)
            OVER (PARTITION BY serie_code_filled ORDER BY ref_date) -1
        ) AS ipca_mom,

        -- YoY
        100.0 * (ipca_nivel_filled/ LAG(ipca_nivel_filled, 12)
            OVER (PARTITION BY serie_code_filled ORDER BY ref_date) -1
        ) AS ipca_yoy
    
    FROM fill_b
)

SELECT * from ipca_metrics ORDER BY ref_date
