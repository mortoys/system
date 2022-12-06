from datetime import datetime, timedelta, time, date
# import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float
# from tqdm import tqdm

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset

# from database import tushare_api as api

# 交易日每天15点～16点之间
class TushareRepurchase(DailyDataset, TushareMixin):
    """回购股票数据"""

    name = "tushare-repurchase"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        # Column("ann_date", Date, comment='公告日期'),
        Column("end_date", Date, comment='截止日期'),
        Column("proc", String, comment='进度', primary_key=True),
        Column("exp_date", String, comment='过期日期'),
        Column("vol", Float, comment='回购数量'),
        Column("amount", Float, comment='回购金额'),
        Column("high_limit", Float, comment='回购最高价'),
        Column("low_limit", Float, comment='回购最低价'),
    ]
    schedule_start_date = '2013-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "ALL"

    def fetch(self):
        data = self.api.repurchase(ann_date=self.ts_date)

        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data = data.sort_values('end_date').drop_duplicates(["ts_code", "ann_date", "proc"], keep='last')

        data = data.rename(columns=dict(ann_date = 'trade_date'))

        return data