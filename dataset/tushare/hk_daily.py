from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareHKDaily(DailyDataset, TushareMixin):
    """获取港股每日增量和历史行情"""

    name = "tushare-hk_daily"
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
    schedule_start_date = '2010-01-04'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "XHKG"

    def fetch(self):
        return self.api.hk_daily(trade_date=self.ts_date)
