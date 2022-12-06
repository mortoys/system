from database import tushare_api
from datetime import datetime, timedelta, date
# from dataset.type import DatasetDailyTicker, DatasetDailyIndex, DatasetDaily, DatasetTicker
import numpy as np
import pandas as pd

class TushareMixin:
    api = tushare_api
    domain = "tushare"

    @property
    def ts_date(self):
        return self.date.strftime("%Y%m%d")

    def current_period(self):
        years = list(range(2000, 2030))
        quarters = ['03-31', '06-30', '09-30', '12-31']

        times = [date.fromisoformat(str(year) + '-' + quarter) for year in years for quarter in quarters]
        cond = pd.Series(map(lambda d: d < date.today(), times))
        return times[np.argmin(cond)-1].strftime("%Y%m%d")

# class TushareDaily(TushareMixin, DatasetDaily):
#     col_time = 'trade_date'
#     primary_key = ['trade_date']

# class TushareTicker(TushareMixin, DatasetTicker):
#     col_ticker = 'ts_code'
#     primary_key = ['ts_code']

# class TushareDailyTicker(TushareMixin, DatasetDailyTicker):
#     col_time = 'trade_date'
#     col_ticker = 'ts_code'
#     primary_key = ['trade_date', 'ts_code']

# class TushareDailyIndex(TushareMixin, DatasetDailyIndex):
#     col_time = 'trade_date'
#     col_ticker = 'ts_code'
#     primary_key = ['trade_date', 'ts_code']