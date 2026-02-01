import duckdb
import pandas as pd
from pmdarima import auto_arima
from datetime import timedelta
from pathlib import Path

# DuckDB
PROJECT_ROOT = Path(__file__).resolve().parents[1]

duck_path = PROJECT_ROOT / "data" / "duckdb" / "ipea.duckdb"

con = duckdb.connect(str(duck_path))

# Puxando IPCA
df = con.execute(
    '''
    SELECT
        ref_date,
        ipca_nivel_filled as ipca_nivel
    FROM int_ipca_metrics
    WHERE ipca_nivel_filled IS NOT NULL
    ORDER BY ref_date ASC
'''
).df()

df['ref_date'] = pd.to_datetime(df['ref_date'])
df = df.set_index('ref_date')
df = df.asfreq("MS")

# auto-arima
model = auto_arima(
    df['ipca_nivel'],
    max_p = 3, max_q = 3,
    d = 1,
    seasonal=False,
    stepwise=True,
    suppress_warnings=True,
    error_action="ignore"
)

steps = 12
forecasts = model.predict(
    n_periods = steps
)
last_date = df.index.max()
future_dates = pd.date_range(
    start = last_date + pd.offsets.MonthBegin(1),
    periods = steps,
    freq = 'MS'
)

forecast_df = pd.DataFrame(
    {
        'ref_date': future_dates,
        'ipca_nivel': forecasts,
        'tipo': 'Forecast'
    }
)

# Combinando c df (histórico) p/ criar MoM e YoY aqui na pipeline
df_hist = df.copy()
df_hist['tipo'] = 'Hist'

df_full = pd.concat([df_hist[['ipca_nivel', 'tipo']], forecast_df.set_index('ref_date')[['ipca_nivel', 'tipo']]])

df_full['ipca_mom'] = df_full['ipca_nivel'].pct_change(1)*100
df_full['ipca_yoy'] = df_full['ipca_nivel'].pct_change(12)*100

df_final = df_full[df_full['tipo'] == 'Forecast'].copy()
df_final['model_arima'] = str(model.order)


# salvando no DuckDB
print(df_final.head(5))

con.register('df_final', df_final.reset_index())
con.execute('CREATE OR REPLACE TABLE ext_ipca_forecast AS SELECT * FROM df_final')
print(
    f'Criado forecast para ipca_nivel a partir de {last_date}'
)

con.close()
