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
    Column("revenue", Float, comment="营业收入(元)"),
    Column("operate_profit", Float, comment="营业利润(元)"),
    Column("total_profit", Float, comment="利润总额(元)"),
    Column("n_income", Float, comment="净利润(元)"),
    Column("total_assets", Float, comment="总资产(元)"),
    Column("total_hldr_eqy_exc_min_int", Float, comment="股东权益合计(不含少数股东权益)(元)"),
    Column("diluted_eps", Float, comment="每股收益(摊薄)(元)"),
    Column("diluted_roe", Float, comment="净资产收益率(摊薄)(%)"),
    Column("yoy_net_profit", Float, comment="去年同期修正后净利润"),
    Column("bps", Float, comment="每股净资产"),
    Column("yoy_sales", Float, comment="同比增长率:营业收入"),
    Column("yoy_op", Float, comment="同比增长率:营业利润"),
    Column("yoy_tp", Float, comment="同比增长率:利润总额"),
    Column("yoy_dedu_np", Float, comment="同比增长率:归属母公司股东的净利润"),
    Column("yoy_eps", Float, comment="同比增长率:基本每股收益"),
    Column("yoy_roe", Float, comment="同比增减:加权平均净资产收益率"),
    Column("growth_assets", Float, comment="比年初增长率:总资产"),
    Column("yoy_equity", Float, comment="比年初增长率:归属母公司的股东权益"),
    Column("growth_bps", Float, comment="比年初增长率:归属于母公司股东的每股净资产"),
    Column("or_last_year", Float, comment="去年同期营业收入"),
    Column("op_last_year", Float, comment="去年同期营业利润"),
    Column("tp_last_year", Float, comment="去年同期利润总额"),
    Column("np_last_year", Float, comment="去年同期净利润"),
    Column("eps_last_year", Float, comment="去年同期每股收益"),
    Column("open_net_assets", Float, comment="期初净资产"),
    Column("open_bps", Float, comment="期初每股净资产"),
    Column("perf_summary", String, comment="业绩简要说明"),
    Column("is_audit", Integer, comment="是否审计： 1是 0否"),
    Column("remark", String, comment="备注"),
]

# 财报数据
class TushareExpress(DailyUpdate, TushareMixin):
    """业绩快报"""

    name = "tushare-express"

    schedule_start_date = (date.today() - timedelta(days=1)).isoformat()
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    columns = cols

    def fetch(self):
        period = self.params['period'] if 'period' in self.params else self.current_period()
        logger.yellow('PERIOD: ' + period)

        data = self.api.express_vip(period=period)
        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data = data.rename(columns=dict(ann_date = 'trade_date'))
        data = data.drop_duplicates(["ts_code", "trade_date"], keep='last')
        data = data.set_index(["ts_code", "trade_date"])
        return data

if __name__ == '__main__':
    updater = TushareExpress()

    years = list(range(2001, 2023))
    quarters = ['0331', '0630', '0930', '1231']
    data = pd.DataFrame()

    for year in years:
        for quarter in quarters:
            period = str(year) + quarter
            updater.update(date=date.today().isoformat(), period=period)
            while updater.status == 'error':
                updater.update(date=date.today().isoformat(), period=period)