from datetime import datetime, timedelta, time, date
import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float
from tqdm import tqdm

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset, DailyUpdate

from database import logger, tushare_api as api

# 交易日每天15点～16点之间
class TushareStkHoldernumber(DailyUpdate, TushareMixin):
    """上市公司股东户数数据"""

    name = "tushare-stk_holdernumber"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        # Column("ann_date", Date, primary_key=True),
        Column("end_date", Date, primary_key=True),
        Column("holder_num", Integer, comment='股东户数'),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    def fetch(self):
        data = self.api.stk_holdernumber(end_date=self.ts_date).dropna()

        data = data.drop_duplicates(['ts_code', 'ann_date', 'end_date'], keep='last')

        data = data.rename(columns=dict(ann_date = 'trade_date'))

        return data.set_index(['ts_code', 'trade_date', 'end_date'])

if __name__ == '__main__':
    holders = TushareStkHoldernumber()

    years = list(range(2009, 2022))
    months = list(range(1, 13))
    data = pd.DataFrame()

    for year in years:
        for month in months:
            end_date = date(year, month, 1)
            logger.yellow('PERIOD: ' + end_date.strftime("%Y-%m-%d"))
            holders.update(date=end_date)
            while holders.status == 'error':
                holders.update(date=end_date)