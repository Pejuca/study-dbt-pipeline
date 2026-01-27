WITH datas AS (
    SELECT DISTINCT 
        ref_date
    FROM {{
        ref('int_ipca_metrics')
    }}
)

SELECT
    ref_date,
    EXTRACT(year FROM ref_date) AS year,
    EXTRACT(month FROM ref_date) AS month,
    STRFTIME(ref_date, '%Y/%m') AS year_month,
    STRFTIME(ref_date, '%B') AS month_name
FROM datas
ORDER BY ref_date ASC