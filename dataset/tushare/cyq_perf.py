from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareCyqPerf(DailyDataset, TushareMixin):
    """获取A股每日筹码平均成本和胜率情况"""

    name = "tushare-cyq_perf"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("his_low", Numeric(scale=2), comment="历史最低价"),
        Column("his_high", Numeric(scale=2), comment="历史最高价"),
        Column("cost_5pct", Float, comment="5分位成本"),
        Column("cost_15pct", Float, comment="15分位成本"),
        Column("cost_50pct", Float, comment="50分位成本"),
        Column("cost_85pct", Float, comment="85分位成本"),
        Column("cost_95pct", Float, comment="95分位成本"),
        Column("weight_avg", Float, comment="加权平均成本"),
        Column("winner_rate", Float, comment="胜率"),
    ]
    schedule_start_date = '2005-01-17'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.cyq_perf(trade_date=self.ts_date)
