from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareBakBasic(DailyDataset, TushareMixin):
    """备用基础列表"""

    name = "tushare-bak_basic"
    columns = [
        Column('trade_date', Date, primary_key=True),
        Column('ts_code', String, primary_key=True),
        Column('name', String),
        Column('industry', String, comment='行业'),
        Column('area', String, comment='地域'),
        Column('pe', Float, comment='市盈率（动）'),
        Column('float_share', Float, comment='流通股本（万）'),
        Column('total_share', Float, comment='总股本（万）'),
        Column('total_assets', Float, comment='总资产（万）'),
        Column('liquid_assets', Float, comment='流动资产（万）'),
        Column('fixed_assets', Float, comment='固定资产（万）'),
        Column('reserved', Float, comment='公积金'),
        Column('reserved_pershare', Float, comment='每股公积金'),
        Column('eps', Float, comment='每股收益'),
        Column('bvps', Float, comment='每股净资产'),
        Column('pb', Float, comment='市净率'),
        Column('list_date', Integer, comment='上市日期'),
        Column('undp', Float, comment='未分配利润'),
        Column('per_undp', Float, comment='每股未分配利润'),
        Column('rev_yoy', Float, comment='收入同比（%）'),
        Column('profit_yoy', Float, comment='利润同比（%）'),
        Column('gpr', Float, comment='毛利率（%）'),
        Column('npr', Float, comment='净利润率（%）'),
        Column('holder_num', Integer, comment='股东人数'),
    ]
    schedule_start_date = '2016-08-09'
    schedule_delay = timedelta(days=0)
    schedule_time = time(23, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.bak_basic(trade_date=self.ts_date)