from datetime import datetime, timedelta, time, date
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareBlockTrade(DailyDataset, TushareMixin):
    """大宗交易"""

    name = "tushare-block_trade"
    columns = [
        Column("ts_code", String),
        Column("trade_date", Date),
        Column("price", Float, comment='成交价'),
        Column("vol", Float, comment='成交量（万股）'),
        Column("amount", Float, comment='成交金额'),
        Column("buyer", String, comment='买方营业部'),
        Column("seller", String, comment='卖方营业部'),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(15, 30)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.block_trade(trade_date=self.ts_date)