from datetime import datetime, timedelta, time, date
import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float
from tqdm import tqdm

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyUpdate

from database import tushare_api as api

class TushareTop10Holders(DailyUpdate, TushareMixin):
    """前十大股东数据"""

    name = "tushare-top10_holders"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("end_date", Date),
        Column("holder_name", String, comment='股东名称', primary_key=True),
        Column("hold_amount", Float, comment='持有数量（股）'),
        Column("hold_ratio", Float, comment='持有比例'),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    def fetch(self):
        data = pd.DataFrame()

        if 'ts_code' in self.params:
            data = self.api.top10_holders(ts_code=self.params['ts_code'])
        else:
            data = self.api.top10_holders()

        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data = data.sort_values('end_date').drop_duplicates(["ts_code", "ann_date", "holder_name"], keep='last')

        data = data.rename(columns=dict(ann_date = 'trade_date'))

        return data.set_index(["ts_code", "trade_date", "holder_name"])

if __name__ == '__main__':
    holders = TushareTop10Holders()

    codes = api.stock_basic()['ts_code']

    for code in tqdm(codes):
        holders.update(ts_code=code, date=date.today().isoformat())
        if holders.status == 'error':
            holders.update(ts_code=code, date=date.today().isoformat())
        if holders.status == 'error':
            holders.update(ts_code=code, date=date.today().isoformat())
        if holders.status == 'error':
            holders.update(ts_code=code, date=date.today().isoformat())