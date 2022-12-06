import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from database import logger
from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyUpdate

cols = [
    Column("ts_code", String, comment="TS股票代码", primary_key=True),
    Column("trade_date", Date, comment="公告日期", primary_key=True),
    Column("end_date", Date, comment="报告期"),
    Column("type", String, comment="业绩预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)"),
    Column("p_change_min", Float, comment="预告净利润变动幅度下限（%）"),
    Column("p_change_max", Float, comment="预告净利润变动幅度上限（%）"),
    Column("net_profit_min", Float, comment="预告净利润下限（万元）"),
    Column("net_profit_max", Float, comment="预告净利润上限（万元）"),
    Column("last_parent_net", Float, comment="上年同期归属母公司净利润"),
    Column("first_ann_date", String, comment="首次公告日"),
    Column("summary", String, comment="业绩预告摘要"),
    Column("change_reason", String, comment="业绩变动原因"),
]

# 财报数据
class TushareForecast(DailyUpdate, TushareMixin):
    """业绩预告"""

    name = "tushare-forecast"

    schedule_start_date = (date.today() - timedelta(days=1)).isoformat()
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    columns = cols

    def fetch(self):
        period = self.params['period'] if 'period' in self.params else self.current_period()
        logger.yellow('PERIOD: ' + period)

        data = self.api.forecast_vip(period=period)
        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data = data.rename(columns=dict(ann_date = 'trade_date'))
        data = data.drop_duplicates(["ts_code", "trade_date"], keep='last')
        data = data.set_index(["ts_code", "trade_date"])
        return data

if __name__ == '__main__':
    updater = TushareForecast()

    years = list(range(2021, 2023))
    quarters = ['0331', '0630', '0930', '1231']
    data = pd.DataFrame()

    for year in years:
        for quarter in quarters:
            period = str(year) + quarter
            updater.update(date=date.today().isoformat(), period=period)
            while updater.status == 'error':
                updater.update(date=date.today().isoformat(), period=period)