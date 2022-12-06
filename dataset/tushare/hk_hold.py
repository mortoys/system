from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, BigInteger, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareHkHold(DailyDataset, TushareMixin):
    """获取沪深港股通持股明细，数据来源港交所"""

    name = "tushare-hk_hold"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("code", String, comment="原始代码"),
        Column("name", String, comment="股票名称"),
        Column("vol", BigInteger, comment="持股数量(股)"),
        Column("ratio", Float, comment="持股占比（%），占已发行股份百分比"),
        Column("exchange", String, comment="类型：SH沪股通SZ深股通HK港股通"),
    ]
    schedule_start_date = '2016-06-29'
    schedule_delay = timedelta(days=0)
    schedule_time = time(22, 0)
    schedule_market = "XHKG"

    def fetch(self):
        return self.api.hk_hold(trade_date=self.ts_date)
