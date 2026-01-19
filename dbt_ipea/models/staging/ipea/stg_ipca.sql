WITH raw_t AS (

    SELECT
        date_trunc('month', date)::date as ref_date,
        upper(trim(serie)) AS serie_code,
        cast(value AS double) AS ipca_nivel
    FROM {{
        source('ipea', 'raw_ipca') 
    }} -- ipea.duckdb , table raw_ipca

),

dates_corrected AS (
    SELECT
        generate_series(
            (SELECT min(ref_date) FROM raw_t),
            (SELECT max(ref_date) FROM raw_t),
            interval '1 month'
        )::date AS ref_date

)

SELECT
    sp.ref_date,
    raw_t.serie_code,
    raw_t.ipca_nivel
from dates_corrected AS sp
LEFT JOIN raw_t
    ON raw_t.ref_date = sp.ref_date
ORDER BY sp.ref_date ASC