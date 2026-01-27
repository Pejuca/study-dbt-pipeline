import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

duck_path = PROJECT_ROOT / "data" / "duckdb" / "ipea.duckdb"

duck_path.parent.mkdir(
    parents = True,
    exist_ok = True
)

con = duckdb.connect(
    str(duck_path)
)
con.execute('SELECT 1')

print('DuckDB initialized')