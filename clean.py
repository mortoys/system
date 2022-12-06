from functools import reduce

import pandas as pd
import numpy as np

from engine.clean import Clean

class AStockDaily(Clean):
    asset = 'astock'
    freq = 'daily'

    def process(self):
        self.perpare()
        self.axis()

    def perpare(self):
        data = reduce(
            lambda x, y: pd.merge(x, y,
                on = ['trade_date', 'ts_code'],
                how = 'outer'
            ), 
            self.loads([
                'tushare/daily',
                'tushare/adj_factor',
                'tushare/daily_basic',
                'tushare/moneyflow',
                'tushare/margin_detail',
            ]))

        data = data.drop(['name', 'pre_close', 'change', 'pct_chg'], axis=1)

        hk_hold = self.load('tushare/hk_hold')
        hk_hold = hk_hold.rename(dict(
            vol = 'hk_vol',
            ratio = 'hk_ratio'
        ), axis=1).drop(['name', 'code', 'exchange'], axis=1)

        data = pd.merge(
            data, hk_hold, 
            on = ['trade_date', 'ts_code'], 
            how = 'left')

        self.data = data.rename(dict(close_x = 'close'), axis=1).drop('close_y', axis=1)

    def complete(self):
        self.data = self.data.groupby('asset').apply(lambda s: s.ffill())

    def axis(self):
        data = self.data.rename(dict(
            trade_date = 'date',
            ts_code = 'asset',
        ), axis=1)

        date = pd.to_datetime(data['date']).astype('datetime64[ns]')
        data['date'] = date#.dt.tz_localize('Asia/Shanghai') + pd.Timedelta(hours=15)
        data['asset'] = data['asset'].astype('category')

        self.data = data.sort_values(['date', 'asset']).set_index(['date', 'asset'])

class AStockSeason(Clean):
    asset = 'astock'
    freq = 'season'

    def align_data(self, df):
        pass

    def process(self):
        self.perpare()
        self.axis()

    def perpare(self):
        self.data = self.load('tushare/fina_indicator')

    def complete(self):
        self.data = self.data.groupby('asset').apply(lambda s: s.ffill())

    def axis(self):
        data = self.data.rename(dict(
            trade_date = 'date',
            ts_code = 'asset',
        ), axis=1)

        date = pd.to_datetime(data['date']).astype('datetime64[ns]')
        data['date'] = date#.dt.tz_localize('Asia/Shanghai') + pd.Timedelta(hours=15)
        data['asset'] = data['asset'].astype('category')

        self.data = data.sort_values(['date', 'asset']).set_index(['date', 'asset'])

class AIndexDaily(Clean):
    asset = 'aindex'
    freq = 'daily'

    def process(self):
        self.perpare()
        self.axis()

    def perpare(self):
        self.data = reduce(
            lambda x, y: pd.merge(x, y,
                on = ['trade_date', 'ts_code'],
                how = 'outer'
            ), 
            self.loads([
                'tushare/index_daily',
                'tushare/index_dailybasic',
            ]))

        self.data = self.data.drop(['pct_chg', 'pre_close', 'change'], axis=1)

    def complete(self):
        self.data = self.data.groupby('asset').apply(lambda s: s.ffill())

    def axis(self):
        data = self.data.rename(dict(
            trade_date = 'date',
            ts_code = 'asset'
        ), axis=1)

        date = pd.to_datetime(data['date']).astype('datetime64[ns]')
        data['date'] = date#.dt.tz_localize('Asia/Shanghai') + pd.Timedelta(hours=15)
        data['asset'] = data['asset'].astype('category')

        self.data = data.sort_values(['date', 'asset']).set_index(['date', 'asset'])

class CBDaily(Clean):
    asset = 'cb'
    freq = 'daily'

    def process(self):
        self.perpare()
        self.axis()

    def perpare(self):
        cb_daily = self.load('tushare/cb_daily')
        cb_detail = self.load('jsl/cbnew_detail_hist')

        cb_detail['trade_date'] = pd.to_datetime(cb_detail['last_chg_dt'])
        cb_detail = cb_detail.drop(['last_chg_dt'], axis=1)

        cb_daily['trade_date'] = cb_daily['trade_date'].astype('datetime64[ns]')
        cb_daily['bond_id'] = cb_daily['ts_code'].str.slice(0,6)

        self.data = pd.merge(cb_daily, cb_detail, on=['bond_id', 'trade_date'])
        self.data = self.data.drop(['bond_id', 'amt_change', 'pre_close', 'pct_chg', 'change'], axis=1)

    def complete(self):
        self.data = self.data.groupby('asset').apply(lambda s: s.ffill())

    def axis(self):
        data = self.data.rename(dict(
            trade_date = 'date',
            ts_code = 'asset'
        ), axis=1)

        date = pd.to_datetime(data['date']).astype('datetime64[ns]')
        data['date'] = date#.dt.tz_localize('Asia/Shanghai') + pd.Timedelta(hours=15)
        data['asset'] = data['asset'].astype('category')

        self.data = data.sort_values(['date', 'asset']).set_index(['date', 'asset'])


if __name__ == '__main__':
    AStockDaily().dump()
    AStockSeason().dump()
    AIndexDaily().dump()
    CBDaily().dump()