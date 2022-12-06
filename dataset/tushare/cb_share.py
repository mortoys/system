import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import Dataset
from dataset.tushare.__mixin__ import TushareMixin


class TushareCBShare(Dataset, TushareMixin):
    """可转债转股结果"""

    name = "tushare-cb_share"
    columns = [
        Column('ts_code', String, primary_key=True, index=True),

        Column('bond_short_name', String, comment='债券简称'),
        Column('publish_date', String, comment='公告日期'),
        Column('end_date', String, primary_key=True, index=True, comment='统计截止日期'),
        Column('issue_size', Float, comment='可转债发行总额'),
        Column('convert_price_initial', Float, comment='初始转换价格'),
        Column('convert_price', Float, comment='本次转换价格'),
        Column('convert_val', Float, comment='本次转股金额'),
        Column('convert_vol', Float, comment='本次转股数量'),
        Column('convert_ratio', Float, comment='本次转股比例'),
        Column('acc_convert_val', Float, comment='累计转股金额'),
        Column('acc_convert_vol', Float, comment='累计转股数量'),
        Column('acc_convert_ratio', Float, comment='累计转股比例'),
        Column('remain_size', Float, comment='可转债剩余金额'),
        Column('total_shares', Float, comment='转股后总股本'),
    ]

    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        data = self.api.cb_share(ts_code=self.params['ticker'])
        return data

if __name__ == '__main__':
    cbShare = TushareCBShare()

    from database import query
    from database import tushare_api as api

    data = query('tushare-cb_basic')

    for code in data['ts_code'].values:
        cbShare.update(ticker = code)