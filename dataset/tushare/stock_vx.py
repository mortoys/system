from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float
import pandas as pd

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareStockVX(DailyDataset, TushareMixin):
    """小沛估值因子"""

    name = "tushare-stock_vx"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("level1", Float, comment="4评级：1(便宜)、2(合理)、3(贵)、4(很贵)"),
        Column("level2", Float, comment="8评级：1,2(便宜)、3,4(合理)、5,6(贵)、7,8(很贵)"),
        Column("vx_life_v_l4", Float, comment="估值长优4条线，根据level1的评级，公司上市后每一天的估值评级平均"),
        Column("vx_3excellent_v_l4", Float, comment="估值3优4条线，根据level1的评级，最新季度的估值评级、近5季度的估值评级平均、上市后的估值评级平均，短中长的估值评级再取一次平均形成三优指标"),
        Column("vx_past_5q_avg_l4", Float, comment="估值4条线近5季平均，根据level1的评级，最近五季度估值评级平均"),
        Column("vx_grow_worse_v_l4", Float, comment="估值进退步-估值4条线,根据level1的评级，最新的估值评级与最近5Q平均的比"),
        Column("vx_life_v_l8", Float, comment="估值长优8条线,根据level2的评级，公司上市后每一季度的估值评级平均"),
        Column("vx_3excellent_v_l8", Float, comment="估值3优8条线,根据level2的评级，最新季度的估值评级、近5季度的估值评级平均、上市后的估值评级平均，短中长的估值评级再取一次平均形成三优指标"),
        Column("vx_past_5q_avg_l8", Float, comment="估值8条线近5季平均,根据level2的评级，最近五季度估值评级平均"),
        Column("vx_grow_worse_v_l8", Float, comment="估值进退步-估值8条线,根据level2的评级，最新的估值评级与最近5Q平均的比较"),
        Column("vxx", Float, comment="个股最新估值与亚洲同类股票相较后的标准差，按因子排序，数值越大代表估值越贵"),
        Column("vs", Float, comment="个股最新估值与亚洲同类股票自己相较后的标准差，按因子排序，数值越大代表估值越贵"),
        Column("vz11", Float, comment="个股最新估值与亚洲同类股票主行业相较后的标准差，按因子排序，数值越大代表估值越贵"),
        Column("vz24", Float, comment="个股最新估值与亚洲同类股票次行业相较后的标准差，按因子排序，数值越大代表估值越贵"),
        Column("vz_lms", Float, comment="个股最新估值与亚洲同类股票市值分类相较后的标准差，按因子排序，数值越大代表估值越贵"),
    ]
    schedule_start_date = '2014-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return pd.concat([
            self.api.stock_vx(trade_date=self.ts_date),
            self.api.stock_vx(trade_date=self.ts_date, offset=2000)
        ])
