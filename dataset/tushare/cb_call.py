import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import DailyRefresh
from .__mixin__ import TushareMixin


class TushareCBCall(DailyRefresh, TushareMixin):
    """获取可转债到期赎回、强制赎回等信息"""

    name = "tushare-cb_call"
    columns = [
        Column('ts_code', String, primary_key=True, index=True),

        Column('call_type', String, comment='赎回类型：到赎、强赎'),
        Column('is_call', String, comment='是否赎回：公告到期赎回、公告强赎、公告不强赎'),
        Column('ann_date', String, primary_key=True, index=True, comment='公告日期'),
        Column('call_date', String, comment='赎回日期'),
        Column('call_price', Float, comment='赎回价格(含税，元/张)'),
        Column('call_price_tax', Float, comment='赎回价格(扣税，元/张)'),
        Column('call_vol', Float, comment='赎回债券数量(张)'),
        Column('call_amount', Float, comment='赎回金额(万元)'),
        Column('payment_date', String, comment='行权后款项到账日'),
        Column('call_reg_date', String, comment='赎回登记日'),
    ]

    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        data = self.api.cb_call()
        return data
