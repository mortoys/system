from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareFundShare(DailyDataset, TushareMixin):
    """获取基金规模数据，包含上海和深圳ETF基金"""

    name = "tushare-fund_share"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("fund_type", String, comment="基金类型"),
        Column("market", String, comment="市场：SH/SZ"),
        Column("fd_share", Float, comment="基金份额（万）"),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "ALL"

    def fetch(self):
        return self.api.fund_share(trade_date=self.ts_date)