import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import DailyRefresh
from .__mixin__ import TushareMixin


class TushareStockBasic(DailyRefresh, TushareMixin):
    """获取基础信息数据，包括股票代码、名称、上市日期、退市日期等"""

    name = "tushare-stock_basic"
    columns = [
        Column('ts_code', String, primary_key=True, index=True),

        Column('symbol', String, comment='股票代码'),
        Column('name', String, comment='股票名称'),
        Column('area', String, comment='地域'),
        Column('industry', String, comment='所属行业'),
        Column('fullname', String, comment='股票全称'),
        Column('enname', String, comment='英文全称'),
        Column('cnspell', String, comment='拼音缩写'),
        Column('market', String, comment='市场类型（主板/创业板/科创板/CDR）'),
        Column('exchange', String, comment='交易所代码'),
        Column('curr_type', String, comment='交易货币'),
        Column('list_status', String, comment='上市状态 L上市 D退市 P暂停上市'),
        Column('list_date', String, comment='上市日期'),
        Column('delist_date', String, comment='退市日期'),
        Column('is_hs', String, comment='是否沪深港通标的，N否 H沪股通 S深股通'),
    ]
    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        data = self.api.stock_basic()
        return data
