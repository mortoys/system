from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareUSDaily(DailyDataset, TushareMixin):
    """获取美股每日增量和历史行情"""

    name = "tushare-us_daily"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("close", Numeric(scale=2), comment="收盘价"),
        Column("open", Numeric(scale=2), comment="开盘价"),
        Column("high", Numeric(scale=2), comment="最高价"),
        Column("low", Numeric(scale=2), comment="最低价"),
        Column("pre_close", Numeric(scale=2), comment="昨收价"),
        Column("change", Float, comment="涨跌额"),
        Column("pct_change", Float, comment="涨跌幅"),
        Column("vol", Float, comment="成交量"),
        Column("amount", Float, comment="成交额"),
        Column("vwap", Float, comment="平均价"),
        Column("turnover_ratio", Float, comment="换手率"),
        Column("total_mv", Float, comment="总市值"),
        Column("pe", Float, comment="PE"),
        Column("pb", Float, comment="PB"),
    ]
    schedule_start_date = '2010-01-04'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "US"

    def fetch(self):
        return self.api.us_daily(trade_date=self.ts_date)
