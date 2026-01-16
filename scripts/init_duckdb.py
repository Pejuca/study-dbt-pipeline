import duckdb
from pathlib import Path

duck_path = Path(
    'data/duckdb/ipea.duckdb'
)
duck_path.parent.mkdir(
    parents = True,
    exist_ok = True
)

con = duckdb.connect(
    str(duck_path)
)
con.execute('SELECT 1')

print('DuckDB initialized')