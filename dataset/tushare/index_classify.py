import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import DailyRefresh
from .__mixin__ import TushareMixin
from tqdm import tqdm

class TushareIndexClassify(DailyRefresh, TushareMixin):
    """申万行业分类"""

    name = "tushare-index_classify"
    columns = [
        Column('index_code', String, primary_key=True, index=True),
        Column('industry_name', String),
        Column('ts_code', String, primary_key=True, index=True),
        Column('trade_date', Date),
        Column('out_date', String),
    ]
    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        ll = self.api.index_classify(level='L1')

        index = 0
        data = pd.DataFrame()

        while index != len(ll):
            try:
                print(str(index) + ' ' + ll.iloc[index]['index_code'] + ' ' + ll.iloc[index]['industry_name'])
                df = self.api.index_member(index_code=ll.iloc[index]['index_code'])
                df['industry_name'] = ll.iloc[index]['industry_name']
                data = pd.concat([ data, df ])
                index += 1
            except Exception as e:
                print(str(e))

        data = data.rename(columns={
            "con_code": "ts_code",
            "in_date":  "trade_date"
        })

        return data
