import pandas as pd
from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Boolean

from engine import DailyRefresh
from .__mixin__ import TushareMixin


class TushareFundBasic(DailyRefresh, TushareMixin):
    """获取公募基金数据列表，包括场内和场外基金"""

    name = "tushare-fund_basic"
    columns = [
        Column('ts_code', String, comment='基金代码', primary_key=True),
        Column('name', String, comment='简称'),
        Column('management', String, comment='管理人'),
        Column('custodian', String, comment='托管人'),
        Column('fund_type', String, comment='投资类型'),
        Column('found_date', Date, comment='成立日期'),
        Column('due_date', Date, comment='到期日期'),
        Column('list_date', Date, comment='上市时间'),
        Column('issue_date', Date, comment='发行日期'),
        Column('delist_date', Date, comment='退市日期'),
        Column('issue_amount', Float, comment='发行份额(亿)'),
        Column('m_fee', Float, comment='管理费'),
        Column('c_fee', Float, comment='托管费'),
        Column('duration_year', Float, comment='存续期'),
        Column('p_value', Float, comment='面值'),
        Column('min_amount', Float, comment='起点金额(万元)'),
        Column('exp_return', Float, comment='预期收益率'),
        Column('benchmark', String, comment='业绩比较基准'),
        Column('status', String, comment='存续状态D摘牌 I发行 L已上市'),
        Column('invest_type', String, comment='投资风格'),
        Column('type', String, comment='基金类型'),
        Column('trustee', String, comment='受托人'),
        Column('purc_startdate', String, comment='日常申购起始日'),
        Column('redm_startdate', String, comment='日常赎回起始日'),
        Column('market', String, comment='E场内O场外'),
    ]

    schedule_start_date = '2000-01-01'
    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)

    def fetch(self):
        data = self.api.fund_basic(status='L')
        ll = data[(data['fund_type'] != '债券型') & (data['fund_type'] != '货币市场型')]
        return ll
