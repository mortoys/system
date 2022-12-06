from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.tushare.__mixin__ import TushareMixin
from engine import Dataset

# 交易日每天15点～16点之间
class TushareCbPriceChg(Dataset, TushareMixin):
    """可转债转股价变动"""

    name = "tushare-cb_price_chg"
    columns = [
        Column("ts_code", String, comment="转债代码", primary_key=True),
        Column("bond_short_name", String, comment="转债简称"),
        Column("publish_date", Date, comment="公告日期"),
        Column("change_date", Date, comment="变动日期", primary_key=True),
        Column("convert_price_initial", Float, comment="初始转股价格"),
        Column("convertprice_bef", Float, comment="修正前转股价格"),
        Column("convertprice_aft", Float, comment="修正后转股价格"),
    ]
    schedule_start_date = '2006-08-10'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.cb_price_chg(ts_code=self.params['ticker'])

if __name__ == '__main__':
    priceChg = TushareCbPriceChg()

    from database import query

    data = query('tushare-cb_basic')

    for code in data['ts_code'].values:
        priceChg.update(ticker = code)