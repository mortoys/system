from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset, DailyUpdate

class TushareFundNav(DailyDataset, TushareMixin):
    """获取公募基金净值数据"""

    name = "tushare-fund_nav"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, comment="公告日期"),

        # Column("ann_date", String, comment="公告日期"),
        Column("nav_date", Date, comment="净值日期", primary_key=True),
        Column("unit_nav", Float, comment="单位净值"),
        Column("accum_nav", Float, comment="累计净值"),
        Column("accum_div", Float, comment="累计分红"),
        Column("net_asset", Float, comment="资产净值"),
        Column("total_netasset", Float, comment="合计资产净值"),
        Column("adj_nav", Float, comment="复权单位净值"),
        Column("update_flag", String, comment="更新标识，0未修改1更正过")
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "ALL"

    def fetch(self):
        data = self.api.fund_nav(nav_date=self.ts_date)

        data = data.sort_values('ann_date').drop_duplicates(["ts_code", "nav_date"], keep='last')

        data = data.rename(columns=dict(ann_date = 'trade_date'))

        # data = data.set_index(["ts_code", "nav_date"])

        return data