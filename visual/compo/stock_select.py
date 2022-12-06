from sqlalchemy.sql.expression import column
from database import query
import streamlit as st
import pandas as pd


def stock_select():
    data = query('tushare-bak_basic', cond=(column('trade_date') == '2021-04-28'))

    industry_list = data['industry'].unique()

    industry = st.sidebar.selectbox('行业', ['全部'] + list(industry_list))

    # st.write(industry)

    # st.write(data[data['industry'] == industry].apply(lambda s: s['ts_code'] + '-' + s['name'], axis=1))

    option_list = pd.DataFrame()
    if industry == '全部':
        option_list = data
    else:
        option_list = data[data['industry'] == industry]

    # option_list = option_list.sort_values('total_assets')

    stock = st.sidebar.selectbox('A股', 
        option_list.sort_values('total_assets', ascending=False).apply(lambda s: s.to_dict(), axis=1),
        format_func = lambda s: s['ts_code'] + ' —— ' + s['name'],
        key = 'ts_code'
    )

    return stock, data[data['industry'] == stock['industry']]['ts_code']