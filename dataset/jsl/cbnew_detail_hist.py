from datetime import datetime, timedelta, time
import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from dataset.jsl.__mixin__ import JSLMixin
from engine import Dataset
from database import logger

class JSLCBNewDetailHist(Dataset, JSLMixin):
    """备用基础列表"""

    name = "jsl-cbnew-detail_hist"
    columns = [
        Column("bond_id", String, comment="", primary_key=True), #  "123102"
        Column("last_chg_dt", Date, comment="", primary_key=True), #  "2021-07-08"
        Column("amt_change", Float, comment=""), #  "-"
        Column("convert_value", Float, comment="转股价值"), #  "206.05"
        Column("curr_iss_amt", Float, comment="存量(亿元)"), #  "6.700"
        Column("premium_rt", Float, comment="转股溢价率"), #  "-15.80%"
        Column("price", Float, comment="收盘价"), #  "173.500"
        Column("stock_volume", Float, comment="成交额(万元)"), #  12554778
        Column("turnover_rt", Float, comment="换手率"), #  "187.38"
        Column("volume", Float, comment="成交量"), #  "224663.26"
        Column("ytm_rt", Float, comment="到期税前收益率"), #  "-5.47%"
    ]
    # schedule_start_date = '2016-08-09'
    # schedule_delay = timedelta(days=0)
    # schedule_time = time(23, 0)
    # schedule_market = "SSE"

    def fetch(self):
        url = "https://www.jisilu.cn/data/cbnew/detail_hist/"+self.params['ticker']+"?___jsl=LST___t={ctime:d}"

        dd = self.request(url, dict(
            rp = "50",
            page = "1"
        ))

        if len(dd) == 0:
            return dd

        dd['last_chg_dt'] = pd.to_datetime(dd['last_chg_dt'])
        dd['premium_rt'] = dd['premium_rt'].apply(self.percentage2float)
        dd['ytm_rt'] = dd['ytm_rt'].apply(self.percentage2float)
        
        dd['volume'] = dd['volume'].apply(self.formatfloat)
        dd['turnover_rt'] = dd['turnover_rt'].apply(self.formatfloat)
        dd['stock_volume'] = dd['stock_volume'].apply(self.formatfloat)
        dd['price'] = dd['price'].apply(self.formatfloat)
        dd['premium_rt'] = dd['premium_rt'].apply(self.formatfloat)
        dd['curr_iss_amt'] = dd['curr_iss_amt'].apply(self.formatfloat)
        dd['curr_iss_amt'] = dd['curr_iss_amt'].apply(self.formatfloat)
        dd['convert_value'] = dd['convert_value'].apply(self.formatfloat)
        dd['amt_change'] = dd['amt_change'].apply(self.formatfloat)

        return dd

if __name__ == '__main__':
    from database import tushare_api as api

    cbNewDetailHist = JSLCBNewDetailHist()

    cbNewDetailHist.login()

    # https://www.jisilu.cn/webapi/cb/list/
    ll = api.cb_basic()

    import time

    t = time.time()
    for ticker in ll['ts_code'].values[::-1]:
        cbNewDetailHist.update(ticker = ticker[:6])

        if(time.time() - t > 5):
            logger.yellow('Wait 5s')
            time.sleep(5)
            t = time.time()