import streamlit as st
import numpy as np
from database import query
from database import column, and_, or_
from datetime import date
from plotnine import ggplot, geom_point, aes, stat_smooth, facet_wrap

def main():
    cons = query('opendata-industry_indicator', cond=(column('trade_date') == date(2021, 9, 30)))

    index_names = cons['index_name'].unique()

    index_name = st.sidebar.selectbox('行业', index_names)

    df = query('opendata-industry_indicator', cond=(column('index_name') == index_name))

    df = df.set_index('trade_date')

    # df['close'] = (1 + df['chg_pct']/100).cumprod()

    # df = df[['close', 'pe', 'pb', 'vwap', 'dividend_yield_ratio', 'turnover_pct']]

    st.line_chart(df['close'])
    st.line_chart(df['pe'])
    st.line_chart(df['pb'])
    # st.line_chart(df['dividend_yield_ratio'])
    st.line_chart(df['vwap'])
    st.line_chart(df['turnover_pct'])