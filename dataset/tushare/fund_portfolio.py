import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from database import logger
from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset

class TushareFundPortfolio(DailyDataset, TushareMixin):
    """获取公募基金持仓数据，季度更新"""

    name = "tushare-fund_portfolio"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, comment="公告日期", primary_key=True),

        # Column("ann_date", String, comment="公告日期"),
        Column("end_date", Date, comment="截止日期"),
        Column("symbol", String, comment="股票代码", primary_key=True),
        Column("mkv", Float, comment="持有股票市值(元)"),
        Column("amount", Float, comment="持有股票数量（股）"),
        Column("stk_mkv_ratio", Float, comment="占股票市值比"),
        Column("stk_float_ratio", Float, comment="占流通股本比例"),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "ALL"

    def fetch(self):
        # data = self.api.fund_portfolio(ann_date=self.ts_date)

        # data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        # data = data.sort_values('end_date').drop_duplicates(["ts_code", "ann_date", "symbol"], keep='last')

        # data = data.rename(columns=dict(ann_date = 'trade_date'))

        # return data

        data = self.api.fund_portfolio(ts_code=self.params['ticker'])

        data = data.sort_values('end_date').drop_duplicates(["ts_code", "ann_date", "symbol"], keep='last')

        data = data.rename(columns=dict(ann_date = 'trade_date'))

        return data

if __name__ == '__main__':
    updater = TushareFundPortfolio()

    from database import query, distinct
    # from database import tushare_api as api

    ll = query('tushare-fund_basic').sort_values('issue_date')['ts_code']

    done = distinct('tushare-fund_portfolio', 'ts_code')['ts_code']

    data = ll[~ll.isin(done)]

    logger.yellow('LEN: ' + str(len(data)))

    a = 0

    for code in data.values:
        logger.yellow('CODE:' + code + ' INDEX: ' + str(a))
        
        updater.update(ticker = code, date=date.today().isoformat())

        a = a+1
