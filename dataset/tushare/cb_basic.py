import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import DailyRefresh
from .__mixin__ import TushareMixin


class TushareCBBasic(DailyRefresh, TushareMixin):
    """获取基础信息数据，包括股票代码、名称、上市日期、退市日期等"""

    name = "tushare-cb_basic"
    columns = [
        Column('ts_code', String, primary_key=True, index=True),

        Column('bond_full_name', String, comment='转债名称'),
        Column('bond_short_name', String, comment='转债简称'),
        Column('cb_code', String, comment='转股申报代码'),
        Column('stk_code', String, comment='正股代码'),
        Column('stk_short_name', String, comment='正股简称'),
        Column('maturity', Float, comment='发行期限（年）'),
        Column('par', Float, comment='面值'),
        Column('issue_price', Float, comment='发行价格'),
        Column('issue_size', Float, comment='发行总额（元）'),
        Column('remain_size', Float, comment='债券余额（元）'),
        Column('value_date', String, comment='起息日期'),
        Column('maturity_date', String, comment='到期日期'),
        Column('rate_type', String, comment='利率类型'),
        Column('coupon_rate', Float, comment='票面利率（%）'),
        Column('add_rate', Float, comment='补偿利率（%）'),
        Column('pay_per_year', Integer, comment='年付息次数'),
        Column('list_date', String, comment='上市日期'),
        Column('delist_date', String, comment='摘牌日'),
        Column('exchange', String, comment='上市地点'),
        Column('conv_start_date', String, comment='转股起始日'),
        Column('conv_end_date', String, comment='转股截止日'),
        Column('first_conv_price', Float, comment='初始转股价'),
        Column('conv_price', Float, comment='最新转股价'),
        Column('rate_clause', String, comment='利率说明'),
        Column('conv_stop_date', String, comment='停止转股日(提前到期)'),

        # Column('put_clause', String, comment='赎回条款'),
        # Column('maturity_put_price', String, comment='到期赎回价格(含税)'),
        # Column('call_clause', String, comment='回售条款'),
        # Column('reset_clause', String, comment='特别向下修正条款'),
        # Column('conv_clause', String, comment='转股条款'),
        # Column('guarantor', String, comment='担保人'),
        # Column('guarantee_type', String, comment='担保方式'),
        # Column('issue_rating', String, comment='发行信用等级'),
        # Column('newest_rating', String, comment='最新信用等级'),
        # Column('rating_comp', String, comment='最新评级机构'),
    ]

    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        data = self.api.cb_basic()
        return data
