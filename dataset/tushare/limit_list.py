from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Enum

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareLimitList(DailyDataset, TushareMixin):
    """获取每日涨跌停股票统计，包括封闭时间和打开次数等数据。"""

    name = "tushare-limit_list"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("name", String, comment="股票名称"),
        Column("close", Float, comment="收盘价"),
        Column("pct_chg", Float, comment="涨跌幅"),
        Column("amp", Float, comment="振幅"),
        Column("fc_ratio", Float, comment="封单金额/日成交金额"),
        Column("fl_ratio", Float, comment="封单手数/流通股本"),
        Column("fd_amount", Float, comment="封单金额"),
        Column("first_time", String, comment="首次涨停时间"),
        Column("last_time", String, comment="最后封板时间"),
        Column("open_times", Integer, comment="打开次数"),
        Column("strth", Float, comment="涨跌停强度"),
        # Column("limit", String, comment="D跌停U涨停"),
        Column("limit", Enum('D','U', name='limit_types'), comment="D跌停U涨停"),
    ]
    schedule_start_date = '2016-02-15'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.limit_list(trade_date=self.ts_date)
