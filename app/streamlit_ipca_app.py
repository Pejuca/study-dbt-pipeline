import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

st.set_page_config(layout = 'wide')

# DuckDB
PROJECT_ROOT = Path(__file__).resolve().parents[1]

duck_path = PROJECT_ROOT / "data" / "duckdb" / "ipea.duckdb"

con = duckdb.connect(str(duck_path))

df = con.execute("""
    select *
    from fct_ipca_forecast
    order by ref_date
""").df()

st.title("📈 IPCA Forecast")

st.dataframe(df.tail(24))

con.close()
