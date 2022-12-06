from datetime import datetime, timedelta, time, date
import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float
from tqdm import tqdm

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset

from database import tushare_api as api

# 交易日每天15点～16点之间
class TushareShareFloat(DailyDataset, TushareMixin):
    """限售股解禁"""

    name = "tushare-share_float"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, comment='解禁日期', primary_key=True),
        Column("ann_date", Date, comment='公告日期', primary_key=True),
        # Column("float_date", Date, comment='解禁日期', primary_key=True),
        Column("float_share", Float, comment='流通股份', primary_key=True),
        Column("float_ratio", Float, comment='流通股份占总股本比率'),
        Column("holder_name", String, comment='股东名称', primary_key=True),
        Column("share_type", String, comment='股份类型'),
    ]
    schedule_start_date = '2013-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "ALL"

    def fetch(self):
        data = self.api.share_float(ann_date=self.ts_date)

        # data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data = data.drop_duplicates(["ts_code", "float_date", "holder_name"], keep='last')

        data = data.rename(columns=dict(float_date = 'trade_date'))

        return data