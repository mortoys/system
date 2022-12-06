from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareMarginDetail(DailyDataset, TushareMixin):
    """获取沪深两市每日融资融券明细"""

    name = "tushare-margin_detail"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("name", String, comment="股票名称 （20190910后有数据）"),
        Column("rzye", Float, comment="融资余额(元)"),
        Column("rqye", Float, comment="融券余额(元)"),
        Column("rzmre", Float, comment="融资买入额(元)"),
        Column("rqyl", Float, comment="融券余量（手）"),
        Column("rzche", Float, comment="融资偿还额(元)"),
        Column("rqchl", Float, comment="融券偿还量(手)"),
        Column("rqmcl", Float, comment="融券卖出量(股,份,手)"),
        Column("rzrqye", Float, comment="融资融券余额(元)"),
    ]
    schedule_start_date = '2010-04-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(19, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.margin_detail(trade_date=self.ts_date)
