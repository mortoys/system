from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每天15点～16点之间
class TushareIndexDailyBasic(DailyDataset, TushareMixin):
    """每日指标数据"""

    '''
    000001.SH 上证指数 2004
    000005.SH 商业指数 2004 最好别用
    000006.SH 地产指数 2004 最好别用
    000016.SH 上证50 2004
    000300.SH 沪深300 2009
    000905.SH 中证500 2009
    399001.SZ 深证成指 2004
    399005.SZ 中小100 2009
    399006.SZ 创业板指 20100601
    399016.SZ 深证创新 20161130 最好别用
    399300.SZ 沪深300 2009 别用 用 000300.SH
    399905.SZ 中证500 2009 别用 用 000905.SH
    '''

    name = "tushare-index_dailybasic"
    columns = [
        Column("trade_date", Date, primary_key=True),
        Column("ts_code", String, primary_key=True),
        Column("total_mv", Float, comment='当日总市值（元）'),
        Column("float_mv", Float, comment='当日流通市值（元）'),
        Column("total_share", Float, comment='当日总股本（股）'),
        Column("float_share", Float, comment='当日流通股本（股）'),
        Column("free_share", Float, comment='当日自由流通股本（股）'),
        Column("turnover_rate", Float, comment='换手率'),
        Column("turnover_rate_f", Float, comment='换手率(基于自由流通股本)'),
        Column("pe", Float, comment='市盈率'),
        Column("pe_ttm", Float, comment='市盈率TTM'),
        Column("pb", Float, comment='市净率'),
    ]
    schedule_start_date = '2009-01-09'
    schedule_delay = timedelta(days=0)
    schedule_time = time(19, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.index_dailybasic(trade_date=self.ts_date)

if __name__ == '__main__':
    from database.conf import tushare_api as api
    from database.conf import meta
    from sqlalchemy import Table
    from datetime import datetime
    
    # table = Table(
    #     "tushare-index_dailybasic",
    #     orimeta,
    #     *TushareIndexDailyBasic.columns,
    #     Column("_utime", DateTime, default=datetime.now),
    #     comment=TushareIndexDailyBasic.__doc__,
    # )
    # table.create(checkfirst=True)

    def sync(name):
        data = api.index_dailybasic(ts_code=name).iloc[::-1]
        data["_utime"] = datetime.now()
        print(name, len(data), list(data['trade_date'].iloc[[0,-1]]))
        data.to_sql(
            "tushare-index_dailybasic",
            con=meta.bind,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )

    tickers = [
        '000001.SH','000005.SH','000006.SH','000016.SH','000300.SH','000905.SH',
        '399001.SZ','399005.SZ','399006.SZ','399016.SZ','399300.SZ','399905.SZ',]
    
    for ticker in tickers:
        sync(ticker)