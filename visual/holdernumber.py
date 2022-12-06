

import streamlit as st
import numpy as np
import pandas as pd
from database import query
from database import column, and_, or_
from datetime import date
from plotnine import ggplot, geom_point, aes, stat_smooth, facet_wrap

from .compo.stock_select import stock_select

def main():
    
    stock, stock_industry = stock_select()

    st.write(stock['industry'] + ' ' + stock['ts_code'] + ' ' + stock['name'])

    # df = query('tushare-stk_holdernumber', cond=(column('ts_code') == stock['ts_code']))

    # df.index = df['trade_date']

    # st.line_chart(df['holder_num'])


    qq = query('tushare-stk_holdernumber', cond=(column('ts_code').in_(stock_industry)))

    df = qq[qq['ts_code'] == stock['ts_code']]

    qq = qq.groupby('trade_date')[['holder_num']].mean()

    df = df.merge(qq, on='trade_date', how='outer')

    df.index = df['trade_date']

    st.line_chart(df[['holder_num_x', 'holder_num_y']])