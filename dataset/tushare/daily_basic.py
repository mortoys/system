from datetime import datetime, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from .__mixin__ import TushareMixin
from engine import DailyDataset

# 交易日每日15点～17点之间
class TushareDailyBasic(DailyDataset, TushareMixin):
    """基本面指标"""

    name = "tushare-daily_basic"
    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("close", Numeric(scale=2)),
        Column("turnover_rate", Float, comment="换手率（%）"),
        Column("turnover_rate_f", Float, comment="换手率（自由流通股）"),
        Column("volume_ratio", Float, comment="量比"),
        Column("pe", Float, comment="市盈率（总市值/净利润， 亏损的PE为空）"),
        Column("pe_ttm", Float, comment="市盈率（TTM，亏损的PE为空）"),
        Column("pb", Float, comment="市净率（总市值/净资产）"),
        Column("ps", Float, comment="市销率"),
        Column("ps_ttm", Float, comment="市销率（TTM）"),
        Column("dv_ratio", Float, comment="股息率 （%）"),
        Column("dv_ttm", Float, comment="股息率（TTM）（%）"),
        Column("total_share", Float, comment="总股本 （万股）"),
        Column("float_share", Float, comment="流通股本 （万股）"),
        Column("free_share", Float, comment="自由流通股本 （万）"),
        Column("total_mv", Float, comment="总市值 （万元）"),
        Column("circ_mv", Float, comment="流通市值（万元）"),
    ]
    schedule_start_date = '2009-12-23'
    schedule_delay = timedelta(days=0)
    schedule_time = time(17, 0)
    schedule_market = "SSE"

    def fetch(self):
        return self.api.daily_basic(trade_date=self.ts_date)
