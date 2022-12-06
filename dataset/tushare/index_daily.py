import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset
from tqdm import tqdm
# 交易日每天15点～16点之间
class TushareIndexDaily(DailyDataset, TushareMixin):
    """每日指标数据"""

    '''
    000001.SH 上证指数 2004
    - 000005.SH 商业指数 2004
    - 000006.SH 地产指数 2004
    000016.SH 上证50 2004
    000300.SH 沪深300 2009
    000905.SH 中证500 2009
    399001.SZ 深证成指 2004
    399005.SZ 中小100 2009
    399006.SZ 创业板指 20100601
    399016.SZ 深证创新 20161130
    - 399300.SZ 沪深300 2009
    - 399905.SZ 中证500 2009
    '''

    name = "tushare-index_daily"

    columns = [
        Column("trade_date", Date, primary_key=True),
        Column("ts_code", String, primary_key=True),
        Column("close", Float, comment='收盘点位'),
        Column("open", Float, comment='开盘点位'),
        Column("high", Float, comment='最高点位'),
        Column("low", Float, comment='最低点位'),
        Column("pre_close", Float, comment='昨日收盘点'),
        Column("change", Float, comment='涨跌点'),
        Column("pct_chg", Float, comment='涨跌幅（%）'),
        Column("vol", Float, comment='成交量（手）'),
        Column("amount", Float, comment='成交额（千元）'),
    ]
    schedule_start_date = '2009-01-09'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        tickers = [
            '000001.SH','000016.SH','000300.SH','000905.SH',
            '399001.SZ','399005.SZ','399006.SZ','399016.SZ',]

        df = pd.DataFrame()
        for ticker in tqdm(tickers):
            dd = pd.DataFrame()
            while len(dd) == 0:
                try:
                    dd = self.api.index_daily(trade_date=self.ts_date, ts_code=ticker)
                except Exception as e:
                    print(str(e))

            df = pd.concat([df, dd])

        return df

if __name__ == '__main__':
    from database.conf import tushare_api as api
    from database.conf import meta
    from sqlalchemy import Table
    from datetime import datetime
    # table = Table(
    #     "tushare-index_daily",
    #     meta,
    #     *TushareIndexDaily.columns,
    #     Column("_utime", DateTime, default=datetime.now),
    #     comment=TushareIndexDaily.__doc__,
    # )
    # table.create(checkfirst=True)

    def sync(name):
        data = api.index_daily(ts_code=name).iloc[::-1]
        data["_utime"] = datetime.now()
        print(name, len(data), list(data['trade_date'].iloc[[0,-1]]))
        data.to_sql(
            "tushare-index_daily",
            con=meta.bind,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )

    tickers = [
        '000001.SH','000005.SH','000006.SH','000016.SH','000300.SH','000905.SH',
        '399001.SZ','399005.SZ','399006.SZ','399016.SZ',]
    
    for ticker in tickers:
        sync(ticker)