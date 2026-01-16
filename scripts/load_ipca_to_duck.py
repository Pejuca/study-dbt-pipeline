import duckdb
from pathlib import Path
import logging
import sys

# Path do duckdb
duck_path = Path(
    'data/duckdb/ipea.duckdb'
)

con = duckdb.connect(str(duck_path))

# sessão do s3
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

con.execute("SET s3_region='us-east-1'")

# Table
# print(
try:
    con.execute('''
            CREATE OR REPLACE TABLE raw_ipca AS
            SELECT
                CAST (date AS DATE) as date,
                serie,
                CAST(value AS DOUBLE) AS value,
                CAST(load_ts AS TIMESTAMP) AS load_ts
            FROM read_csv_auto(
                's3://study-dbt-pipeline-jpzam/raw/ipea/PRECOS12_IPCA12.csv'
            )
        ''')#.fetchall()
    # )
except Exception as e:
    logging.error(f'Erro de envio para DuckDB: {e}')
    sys.exit(1)

# print(
#     con.execute("SELECT COUNT(*) FROM raw_ipca").fetchone()
# )