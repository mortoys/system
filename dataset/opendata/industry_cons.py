import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.opendata.__mixin__ import OpenDataMixin
from engine import DailyRefresh

from opendatatools import swindex

class OpenDataIndustryCons(DailyRefresh, OpenDataMixin):
    """申万指数成分信息"""

    name = "opendata-industry_cons"
    columns = [
        Column("index_code", String, primary_key=True),
        Column("index_name", String),
        Column("ts_code", String, primary_key=True),
        Column("name", String),
        Column("weight", Float),
        Column("start_date", Date),
    ]

    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)
    schedule_market = "SSE"

    def fetch(self):
        df, msg = swindex.get_index_list()
        ll = df[df['section_name'] == '一级行业']
        item = ll.values

        df = pd.DataFrame()
        for index_code, index_name,  start_date, section_name in tqdm(item):
            dd = pd.DataFrame()
            try:
                dd, msg = swindex.get_index_cons(index_code)
                dd['weight'] = dd['weight'].apply(self.formatfloat)
                dd['index_code'] = index_code + '.SI'
                dd['index_name'] = index_name

                dd['stock_code'] = dd['stock_code'].apply(self.to_SHSZ)

                dd = dd.rename(dict(stock_code='ts_code', stock_name='name'), axis=1)

            except Exception as e:
                print(str(e))

            df = pd.concat([df, dd])

        return df