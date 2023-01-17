from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareStockMX(DailyDataset, TushareMixin):
    """小佩动量因子"""

    name = "tushare-stock_mx"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("mx_grade", Float, comment="动能评级"),
        # 综合动能指标后分成4个评等，1(高)、2(中)、3(低)、4(弱)。高：周、月、季、半年趋势方向一致，整体看多；
        # 中：周、月、季、半年趋势方向不一致，但整体偏多；
        # 低：周、月、季、半年趋势方向不一致，但整体偏多；
        # 弱：周、月、季、半年趋势方向一致，整体看空
        Column("com_stock", Float, comment="行业轮动指标"),
        Column("evd_v", Float, comment="速度指标，衡量该个股股价变化的速度"),
        Column("zt_sum_z", Float, comment="极值，短期均线离差值"),
        Column("wma250_z", Float, comment="偏离指标，中期均线偏离度指标"),
    ]
    schedule_start_date = '2014-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.stock_mx(trade_date=self.ts_date)
