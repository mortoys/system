from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, BigInteger, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

class TushareMoneyflow(DailyDataset, TushareMixin):
    """获取沪深A股票资金流向数据"""
    # 小单：5万以下 中单：5万～20万 大单：20万～100万 特大单：成交额>=100万

    name = "tushare-moneyflow"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("buy_sm_vol", BigInteger, comment="小单买入量（手）"),
        Column("buy_sm_amount", Float, comment="小单买入金额（万元）"),
        Column("sell_sm_vol", BigInteger, comment="小单卖出量（手）"),
        Column("sell_sm_amount", Float, comment="小单卖出金额（万元）"),
        Column("buy_md_vol", BigInteger, comment="中单买入量（手）"),
        Column("buy_md_amount", Float, comment="中单买入金额（万元）"),
        Column("sell_md_vol", BigInteger, comment="中单卖出量（手）"),
        Column("sell_md_amount", Float, comment="中单卖出金额（万元）"),
        Column("buy_lg_vol", BigInteger, comment="大单买入量（手）"),
        Column("buy_lg_amount", Float, comment="大单买入金额（万元）"),
        Column("sell_lg_vol", BigInteger, comment="大单卖出量（手）"),
        Column("sell_lg_amount", Float, comment="大单卖出金额（万元）"),
        Column("buy_elg_vol", BigInteger, comment="特大单买入量（手）"),
        Column("buy_elg_amount", Float, comment="特大单买入金额（万元）"),
        Column("sell_elg_vol", BigInteger, comment="特大单卖出量（手）"),
        Column("sell_elg_amount", Float, comment="特大单卖出金额（万元）"),
        Column("net_mf_vol", BigInteger, comment="净流入量（手）"),
        Column("net_mf_amount", Float, comment="净流入额（万元）"),
    ]
    schedule_start_date = '2007-01-04'
    schedule_delay = timedelta(days=0)
    schedule_time = time(18, 30)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.moneyflow(trade_date=self.ts_date)