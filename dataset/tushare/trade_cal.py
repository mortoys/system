import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import DailyRefresh
from .__mixin__ import TushareMixin


class TushareTradeCal(DailyRefresh, TushareMixin):
    """日历数据"""

    name = "tushare-trade_cal"
    columns = [
        Column('exchange', String, primary_key=True, index=True),
        # 交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源
        Column('trade_date', Date, primary_key=True, index=True),
        Column('is_open', Boolean, index=True),
        # Column('pretrade_date', DateTime),
    ]
    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        data = pd.concat([
            self.api.trade_cal(is_open=1, exchange='SSE'),
            # self.api.trade_cal(is_open=1, exchange='SZSE'),
            # self.api.trade_cal(is_open=1, exchange='CFFEX'),
            # self.api.trade_cal(is_open=1, exchange='SHFE'),
            # self.api.trade_cal(is_open=1, exchange='CZCE'),
            # self.api.trade_cal(is_open=1, exchange='DCE'),
            # self.api.trade_cal(is_open=1, exchange='INE'),
        ])
        data['is_open'] = data['is_open'].apply(lambda s: s == 1)
        data = data.rename(columns={"cal_date": "trade_date"})
        return data
