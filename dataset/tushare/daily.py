from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareDaily(DailyDataset, TushareMixin):
    """日线行情 未复权行情"""

    name = "tushare-daily"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("open", Numeric(scale=2)),
        Column("high", Numeric(scale=2)),
        Column("low", Numeric(scale=2)),
        Column("close", Numeric(scale=2)),
        Column("pre_close", Numeric(scale=2)),
        Column("change", Float),
        Column("pct_chg", Float),
        Column("vol", Float),
        Column("amount", Float),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.daily(trade_date=self.ts_date)
#  ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011', api=api, freq='30min')
