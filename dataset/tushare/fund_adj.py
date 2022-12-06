from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareFundADJ(DailyDataset, TushareMixin):
    """基金复权因子"""

    name = "tushare-fund_adj"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("adj_factor", Float),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.fund_adj(trade_date=self.ts_date)