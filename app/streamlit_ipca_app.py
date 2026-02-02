import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path
import plotly.graph_objects as go

st.set_page_config(layout = 'wide')

# DuckDB
PROJECT_ROOT = Path(__file__).resolve().parents[1]

duck_path = PROJECT_ROOT / "data" / "duckdb" / "ipea.duckdb"

con = duckdb.connect(str(duck_path))

df_hist = con.execute(
    '''
    SELECT 
        ref_date,
        ipca_nivel,
        ipca_mom,
        ipca_yoy
    FROM fct_ipca_monthly
    WHERE ref_date >= '2000-01-01'
    ORDER BY ref_date
'''
).df()

df_fcast = con.execute(
        '''
    SELECT 
        ref_date,
        ipca_nivel,
        ipca_mom,
        ipca_yoy
    FROM fct_ipca_forecast
    WHERE ref_date >= '2000-01-01'
    ORDER BY ref_date
'''
).df()


for df in [df_hist, df_fcast]:
    df["ref_date"] = pd.to_datetime(df["ref_date"])

start_date = st.date_input(
    "Data inicial",
    value = pd.to_datetime("2024-01-01"),
    min_value = df_hist["ref_date"].min(),
    max_value = df_fcast["ref_date"].max()
)

df_hist_f = df_hist[df_hist["ref_date"] >= pd.to_datetime(start_date)]
df_fcast_f = df_fcast[df_fcast["ref_date"] >= pd.to_datetime(start_date)]

# Plots
def plot_plotly(title, ycol):
    fig = go.Figure()

    fig.add_trace(
        go. Scatter(
            x = df_hist_f['ref_date'],
            y = df_hist_f[ycol],
            # mode = 'lines',
            name = 'Histórico',
            line = dict(color = 'blue'),
            mode = "lines+markers",
            marker=dict(size = 4)
        ) 
    )

    fig.add_trace(
        go. Scatter(
            x = df_fcast_f['ref_date'],
            y = df_fcast_f[ycol],
            mode = 'lines',
            name = 'Forecast',
            line = dict(color = 'red', dash = 'dash')
            # mode = "lines+markers",
            # marker=dict(size = 3) 
        )
    )

    fig.add_vline(
        x = df_fcast['ref_date'].min(),
        line_dash = 'dot',
        line_color = 'black'
    )

    fig.update_layout(
        title = title,
        template = "simple_white",
        font = dict(family="Arial", size=13),
        title_x = 0.02,
        hovermode = "x unified"
    )

    return fig

st.title("IPCA - Histórico e Forecast")

# Subindo figuras com func. plots
fig_mom = plot_plotly(
    'IPCA YoY (%) - Histórico e Forecast', 'ipca_yoy'
)
st.plotly_chart(fig_mom, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    fig_yoy = plot_plotly(
        'IPCA MoM (%) - Histórico e Forecast', 'ipca_mom'
    )
    st.plotly_chart(fig_yoy, use_container_width=True)

with col2:
    fig_nivel = plot_plotly(
        'IPCA Nível (%) - Histórico e Forecast', 'ipca_nivel'
    )
    st.plotly_chart(fig_nivel, use_container_width=True)



con.close()
