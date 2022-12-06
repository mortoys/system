from datetime import datetime, timedelta, time, date
import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float
from tqdm import tqdm

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset

from database import tushare_api as api

# 交易日每天15点～16点之间
class TushareStkHoldertrade(DailyDataset, TushareMixin):
    """上市公司增减持数据"""

    name = "tushare-stk_holdertrade"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        # Column("ann_date", Date, comment='公告日期'),
        Column("holder_name", String, comment='股东名称', primary_key=True),
        Column("holder_type", String, comment='股东类型G高管P个人C公司'),
        Column("in_de", String, comment='类型IN增持DE减持'),
        Column("change_vol", Float, comment='变动数量'),
        Column("change_ratio", Float, comment='占流通比例（%）'),
        Column("after_share", Float, comment='变动后持股'),
        Column("after_ratio", Float, comment='变动后占流通比例（%）'),
        Column("avg_price", Float, comment='平均价格'),
        Column("total_share", Float, comment='持股总数'),
        Column("begin_date", String, comment='增减持开始日期'),
        Column("close_date", String, comment='增减持结束日期'),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "ALL"

    def fetch(self):
        data = self.api.stk_holdertrade(ann_date=self.ts_date)

        data = data.drop_duplicates(["ts_code", "ann_date", "holder_name"], keep='last')

        data = data.rename(columns=dict(ann_date = 'trade_date'))

        return data