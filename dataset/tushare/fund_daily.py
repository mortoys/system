from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareFundDaily(DailyDataset, TushareMixin):
    """获取场内基金日线行情"""

    name = "tushare-fund_daily"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("open", Float, comment="开盘价(元)"),
        Column("high", Float, comment="最高价(元)"),
        Column("low", Float, comment="最低价(元)"),
        Column("close", Float, comment="收盘价(元)"),
        Column("pre_close", Float, comment="昨收盘价(元)"),
        Column("change", Float, comment="涨跌额(元)"),
        Column("pct_chg", Float, comment="涨跌幅(%)"),
        Column("vol", Float, comment="成交量(手)"),
        Column("amount", Float, comment="成交额(千元)"),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.fund_daily(trade_date=self.ts_date)