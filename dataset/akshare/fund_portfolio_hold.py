import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from database import logger
from dataset.akshare.__mixin__ import AkshareMixin
from engine import DailyDataset

class AkshareFundPortfolio(DailyDataset, AkshareMixin):
    """获取公募基金持仓数据，季度更新"""

    name = "akshare-fund_portfolio"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("end_date", Date, primary_key=True),
        Column("symbol", String, comment="股票代码", primary_key=True),
        Column("股票代码", String),
        Column("基金代码", String),
        Column("年份", Integer),
        Column("季度", Integer),
        Column("股票名称", String),
        Column("占净值比例", Float),
        Column("持股数", Float),
        Column("持仓市值", Float),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    def fetch(self):
        data = self.api.fund_portfolio_hold_em(code=self.params['code'], year=self.params['year'])

        data['基金代码'] = self.params['code']
        data['ts_code'] = self.params['ts_code']

        data['symbol'] = data['股票代码'].apply(lambda s: self.to_SHSZ(s))

        data['年份'] = data['季度'].str.slice(0,4)
        data['季度'] = data['季度'].str.slice(5,6)

        data['end_date'] = data['季度'].apply(lambda s: self.params['year'] + '-' + ['03-31', '06-30', '09-30', '12-31'][int(s)-1])

        # data = data.sort_values('end_date').drop_duplicates(["ts_code", "ann_date", "symbol"], keep='last')

        # data = data.rename(columns=dict(ann_date = 'trade_date'))

        return data.drop('序号', axis=1)

if __name__ == '__main__':
    updater = AkshareFundPortfolio()

    # from database import query, distinct
    # from database import tushare_api as api

    # ll = query('tushare-fund_basic').sort_values('issue_date')['ts_code']

    # done = distinct('tushare-fund_portfolio', 'ts_code')['ts_code']

    # data = ll[~ll.isin(done)]

    ll = pd.read_csv('dataset/akshare/fund.csv', dtype='str')

    logger.yellow('LEN: ' + str(len(ll)))

    
    for year in ['2020','2021']:
        a = 0
        for code, ts_code, name in ll.values:
            logger.yellow('CODE:' + code + ' INDEX: ' + str(a))
            
            updater.update(code = code, ts_code = ts_code, year = year, date=date.today().isoformat())

            a = a+1
