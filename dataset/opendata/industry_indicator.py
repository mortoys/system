import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.opendata.__mixin__ import OpenDataMixin
from engine import DailyDataset

from opendatatools import swindex

class OpenDataIndustryIndicator(DailyDataset, OpenDataMixin):
    """申万指数每日的量化指标"""

    name = "opendata-industry_indicator"
    columns = [
        Column("index_code", String, primary_key=True),
        Column("index_name", String),
        Column("trade_date", Date, primary_key=True),
        Column("close", Float),
        Column("volume", Float),
        Column("chg_pct", Float),
        Column("turn_rate", Float),
        Column("pe", Float),
        Column("pb", Float),
        Column("vwap", Float),
        Column("float_mv", Float),
        Column("avg_float_mv", Float),
        Column("dividend_yield_ratio", Float),
        Column("turnover_pct", Float),
    ]

    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(20, 30)
    schedule_market = "SSE"

    def fetch(self):
        df, msg = swindex.get_index_list()
        ll = df[df['section_name'] == '一级行业']
        tickers = ll['index_code'].values

        df = pd.DataFrame()
        for ticker in tqdm(tickers):
            dd = pd.DataFrame()
            try:
                dd, msg = swindex.get_index_dailyindicator(ticker, self.od_date, self.od_date, 'D')

                dd['index_code'] = dd['index_code'].apply(lambda s: s + '.SI')

                dd['volume'] = dd['volume'].apply(self.formatfloat)
                dd['turn_rate'] = dd['turn_rate'].apply(self.formatfloat)
                dd['pe'] = dd['pe'].apply(self.formatfloat)
                dd['pb'] = dd['pb'].apply(self.formatfloat)
                dd['vwap'] = dd['vwap'].apply(self.formatfloat)
                dd['float_mv'] = dd['float_mv'].apply(self.formatfloat)
                dd['avg_float_mv'] = dd['avg_float_mv'].apply(self.formatfloat)
                dd['dividend_yield_ratio'] = dd['dividend_yield_ratio'].apply(self.formatfloat)
                dd['turnover_pct'] = dd['turnover_pct'].apply(self.formatfloat)

                dd = dd.rename(dict(date='trade_date'), axis=1)

            except Exception as e:
                print(str(e))

            # assert(len(dd) != 0)

            df = pd.concat([df, dd])

        return df

if __name__ == '__main__':
    from opendatatools import swindex
    from database.conf import meta
    from sqlalchemy import Table
    from datetime import datetime
    
    # table = Table(
    #     "opendata-industry_indicator",
    #     meta,
    #     *OpenDataIndustryIndicator.columns,
    #     Column("_utime", DateTime, default=datetime.now),
    #     comment=OpenDataIndustryIndicator.__doc__,
    # )
    # table.create(checkfirst=True)

    def formatfloat(value):
        if value is None or value == '' or value == '-':
            return np.nan
        else:
            return float(value.replace(',',''))

    def sync(code):
        data, msg = swindex.get_index_dailyindicator(code, '2009-12-23', '2021-08-09', 'D')
        # data, msg = swindex.get_index_dailyindicator(code, '2021-07-21', '2021-08-01', 'D')
        data = data.iloc[::-1]

        data['index_code'] = data['index_code'].apply(lambda s: s + '.SI')

        data['volume'] = data['volume'].apply(formatfloat)
        data['turn_rate'] = data['turn_rate'].apply(formatfloat)
        data['pe'] = data['pe'].apply(formatfloat)
        data['pb'] = data['pb'].apply(formatfloat)
        data['vwap'] = data['vwap'].apply(formatfloat)
        data['float_mv'] = data['float_mv'].apply(formatfloat)
        data['avg_float_mv'] = data['avg_float_mv'].apply(formatfloat)
        data['dividend_yield_ratio'] = data['dividend_yield_ratio'].apply(formatfloat)
        data['turnover_pct'] = data['turnover_pct'].apply(formatfloat)

        data = data.rename(dict(date='trade_date'), axis=1)

        data["_utime"] = datetime.now()
        data.to_sql(
            "opendata-industry_indicator",
            con=meta.bind,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )

    df, msg = swindex.get_index_list()
    ll = df[df['section_name'] == '一级行业']

    tickers = ll['index_code'].values
    
    for ticker in tqdm(tickers):
        sync(ticker)