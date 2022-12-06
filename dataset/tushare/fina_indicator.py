cols = [
 'eps',
 'dt_eps',
 'total_revenue_ps',
 'revenue_ps',
 'capital_rese_ps',
 'surplus_rese_ps',
 'undist_profit_ps',
 'extra_item',
 'profit_dedt',
 'gross_margin',
 'current_ratio',
 'quick_ratio',
 'cash_ratio',
 'ar_turn',
 'ca_turn',
 'fa_turn',
 'assets_turn',
 'op_income',
 'ebit',
 'ebitda',
 'fcff',
 'fcfe',
 'current_exint',
 'noncurrent_exint',
 'interestdebt',
 'netdebt',
 'tangible_asset',
 'working_capital',
 'networking_capital',
 'invest_capital',
 'retained_earnings',
 'diluted2_eps',
 'bps',
 'ocfps',
 'retainedps',
 'cfps',
 'ebit_ps',
 'fcff_ps',
 'fcfe_ps',
 'netprofit_margin',
 'grossprofit_margin',
 'cogs_of_sales',
 'expense_of_sales',
 'profit_to_gr',
 'saleexp_to_gr',
 'adminexp_of_gr',
 'finaexp_of_gr',
 'impai_ttm',
 'gc_of_gr',
 'op_of_gr',
 'ebit_of_gr',
 'roe',
 'roe_waa',
 'roe_dt',
 'roa',
 'npta',
 'roic',
 'roe_yearly',
 'roa2_yearly',
 'debt_to_assets',
 'assets_to_eqt',
 'dp_assets_to_eqt',
 'ca_to_assets',
 'nca_to_assets',
 'tbassets_to_totalassets',
 'int_to_talcap',
 'eqt_to_talcapital',
 'currentdebt_to_debt',
 'longdeb_to_debt',
 'ocf_to_shortdebt',
 'debt_to_eqt',
 'eqt_to_debt',
 'eqt_to_interestdebt',
 'tangibleasset_to_debt',
 'tangasset_to_intdebt',
 'tangibleasset_to_netdebt',
 'ocf_to_debt',
 'turn_days',
 'roa_yearly',
 'roa_dp',
 'fixed_assets',
 'profit_to_op',
 'q_saleexp_to_gr',
 'q_gc_to_gr',
 'q_roe',
 'q_dt_roe',
 'q_npta',
 'q_ocf_to_sales',
 'basic_eps_yoy',
 'dt_eps_yoy',
 'cfps_yoy',
 'op_yoy',
 'ebt_yoy',
 'netprofit_yoy',
 'dt_netprofit_yoy',
 'ocf_yoy',
 'roe_yoy',
 'bps_yoy',
 'assets_yoy',
 'eqt_yoy',
 'tr_yoy',
 'or_yoy',
 'q_sales_yoy',
 'q_op_qoq',
 'equity_yoy']

import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from database import logger
from .__mixin__ import TushareMixin
from engine import DailyUpdate

# 财报数据
class TushareFinaIndicator(DailyUpdate, TushareMixin):
    """财务指标数据"""

    name = "tushare-fina_indicator"

    schedule_start_date = (date.today() - timedelta(days=1)).isoformat()
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    columns = [
        Column("ts_code", String, primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("end_date", Date),
        Column('update_flag', String)
    ] + [Column(col, Float) for col in cols]

    def current_period(self):
        years = list(range(2000, 2030))
        quarters = ['03-31', '06-30', '09-30', '12-31']

        times = [date.fromisoformat(str(year) + '-' + quarter) for year in years for quarter in quarters]
        cond = pd.Series(map(lambda d: d < date.today(), times))
        return times[np.argmin(cond)-1].strftime("%Y%m%d")

    def fetch(self):
        period = self.params['period'] if 'period' in self.params else self.current_period()
        logger.yellow('PERIOD: ' + period)

        data = self.api.fina_indicator_vip(period=period)
        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data = data.rename(columns=dict(ann_date = 'trade_date'))
        data = data.drop_duplicates(["ts_code", "trade_date"], keep='last')
        data = data.set_index(["ts_code", "trade_date"])
        return data

if __name__ == '__main__':
    finaIndicator = TushareFinaIndicator()

    years = list(range(2021, 2022))
    quarters = ['0331', '0630', '0930', '1231']
    data = pd.DataFrame()

    for year in years:
        for quarter in quarters:
            period = str(year) + quarter
            finaIndicator.update(date=date.today().isoformat(), period=period)
            while finaIndicator.status == 'error':
                finaIndicator.update(date=date.today().isoformat(), period=period)