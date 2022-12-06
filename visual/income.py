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

    # [研报客](https://yanbaoke.com/search?tag={stock['ts_code'][:6]})
    # [东方财富](http://quote.eastmoney.com/{stock['ts_code'][7:] + stock['ts_code'][:6]}.html)
    st.markdown(f'''
        [萝卜投研](https://robo.datayes.com/v2/stock/{stock['ts_code'][:6]}/overview)
        [发现报告](https://www.fxbaogao.com/rp?keywords={stock['name'][:6].replace(' ', '')})
        [巨潮资讯](http://www.cninfo.com.cn/new/fulltextSearch?keyWord={stock['ts_code'][:6]})
    ''')

    dd = query('tushare-income', cond=(column('ts_code') == stock['ts_code']))

    dd.index = dd['end_date']

    dd['period'] = pd.to_datetime(dd['end_date']).dt.quarter

    dd = dd[dd['period'] == 4]

    st.line_chart(dd[['total_revenue', 'total_cogs']])
    st.line_chart(dd[['operate_profit','ebit', 'n_income']])
    cost = ['total_profit', 'oper_cost', 'int_exp', 'comm_exp', 'biz_tax_surchg', 'sell_exp', 'admin_exp', 'fin_exp', 'assets_impair_loss', ]
    cost_name = ['利润总额', '减:营业成本','减:利息支出','减:手续费及佣金支出','减:营业税金及附加','减:销售费用','减:管理费用','减:财务费用','减:资产减值损失',]

    data = dd[cost].rename(dict(zip(cost, cost_name)), axis=1).fillna(0)

    st.area_chart(data)

    # 毛利润
    # (dd['revenue'] - dd['oper_cost']) / dd['revenue']

    # 营业利润率 = 营业利润 / 营业总收入
    # dd['operate_profit'] / dd['total_revenue']

    # 税前利润率 = 利润总额 / 营业总收入
    # dd['total_profit'] / dd['total_revenue']

    # 营业总成本=营业成本+营业税金及附加+销售费用+管理费用+财务费用+资产减值损失
    # dd['oper_cost'] + 
    # dd['sell_exp'] + dd['fin_exp'] + dd['admin_exp'] + 
    # dd['biz_tax_surchg'] + dd['assets_impair_loss']
    # == dd['total_cogs']

    # 利润总额 ＝ 营业利润＋营业外收入－营业外支出
    # dd['operate_profit'] + dd['non_oper_income'] - dd['non_oper_exp']) / dd['total_profit']

    rr = dd[['operate_profit', 'total_profit']].rename(dict(zip(['operate_profit', 'total_profit'], ['营业利润', '利润总额'])), axis=1)
    st.area_chart(rr['营业利润'] / rr['利润总额'])

    # 净利润 = 利润总额 － 所得税费用
    # (dd['total_profit'] - dd['income_tax']) / dd['n_income']
    # 每股收益 = 净利润 / 总股本