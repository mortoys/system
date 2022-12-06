from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareCbDaily(DailyDataset, TushareMixin):
    """获取可转债行情"""

    name = "tushare-cb_daily"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("pre_close", Numeric(scale=2), comment="昨收盘价(元)"),
        Column("open", Numeric(scale=2), comment="开盘价(元)"),
        Column("high", Numeric(scale=2), comment="最高价(元)"),
        Column("low", Numeric(scale=2), comment="最低价(元)"),
        Column("close", Numeric(scale=2), comment="收盘价(元)"),
        Column("change", Float, comment="涨跌(元)"),
        Column("pct_chg", Float, comment="涨跌幅(%)"),
        Column("vol", Float, comment="成交量(手)"),
        Column("amount", Float, comment="成交金额(万元)"),
    ]
    schedule_start_date = '2006-08-10'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.cb_daily(trade_date=self.ts_date)
