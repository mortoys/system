from datetime import datetime, timedelta, time
import pandas as pd
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float, Time

from dataset.jsl.__mixin__ import JSLMixin
from engine import Dataset
from database import logger

class JSLQDIIList(Dataset, JSLMixin):
    """备用基础列表"""

    name = "jsl-qdii_list"
    columns = [
        Column("fund_id", String, comment="", primary_key=True), #"162416",
        Column("last_est_datetime", DateTime, comment="", primary_key=True), #"2022-01-06 11:34:03",
        Column("discount_rt", Float, comment="溢价率"), #"2.36%",

        Column("fund_nm", String, comment=""), #"香港本地",
        Column("qtype", String, comment=""), #"A",
        Column("issuer_nm", String, comment=""), #"华宝基金",
        # Column("urls", String, comment=""), #"http://www.fsfund.com/funds/162416/index.shtml",
        Column("asset_ratio", Float, comment=""), #"95.000",
        Column("lof_type", String, comment=""), #"INDEX",
        Column("index_nm", String, comment=""), #"恒生香港35",

        Column("price", Float, comment=""), #"0.946",
        Column("price_dt", Date, comment=""), #"2022-01-06",
        Column("increase_rt", Float, comment=""), #"0.00%",

        Column("volume", Float, comment=""), #"0.01",
        Column("stock_volume", Float, comment=""), #"0.0100",
        Column("turnover_rt", Float, comment=""), #"0.01%",
        Column("amount", Integer, comment=""), #195,
        Column("amount_incr", Integer, comment=""), #"1",
        Column("amount_increase_rt", Float, comment=""), #"0.33",

        Column("fund_nav", Float, comment="基金净值"), #"0.9362",
        Column("nav_dt", Date, comment=""), #"2022-01-05",

        Column("last_est_dt", Date, comment=""), #"2022-01-06",
        # Column("last_time", Time, comment=""), #"11:34:03",
        Column("last_est_time", Time, comment=""), #"11:34:03",
        Column("ref_price", Float, comment="跟踪价格"), #"3030.91",
        Column("ref_increase_rt", Float, comment="跟踪收益"), #"-1.23%",
        # Column("ref_increase_rt2", String, comment=""), #"-",
        Column("estimate_value", Float, comment="估计净值"), #"0.9242",
        Column("est_val_dt", Date, comment="估计日期"), #"2022-01-06",
        Column("est_val_increase_rt", Float, comment="估计收益"), #"-1.28%",

        # Column("index_id", String, comment=""), #"Y",
        # Column("apply_fee", String, comment=""), #"1.00%",
        # Column("apply_fee_tips", String, comment=""), #"100万元以下\t1.0%\n100万元(含)-200万元\t0.6%\n200万元(含)以上\t每笔1000元",
        # Column("redeem_fee", String, comment=""), #"1.50%",
        # Column("redeem_fee_tips", String, comment=""), #"7天以内\t1.5%\n7天(含)以上\t0.5%",
        # Column("apply_status", String, comment=""), #"开放",
        # Column("redeem_status", String, comment=""), #"开放",
        # Column("min_amt", String, comment=""), #null,
        # Column("money_cd", String, comment=""), #"HKD",
        # Column("notes", String, comment=""), #"",
        # Column("estimate_value2", String, comment=""), #"-",
        # Column("est_val_dt2", String, comment=""), #"-",
        # Column("est_val_increase_rt2", String, comment=""), #"-",
        # Column("discount_rt2", String, comment=""), #"-",
        # Column("owned", String, comment=""), #0,
        # Column("holded", String, comment=""), #0,
        # Column("cal_tips", String, comment=""), #"",
        # Column("cal_index_id", String, comment=""), #"",
        # Column("est_val_tm2", String, comment=""), #"-",
        # Column("ref_price2", String, comment=""), #"-",
        # Column("apply_redeem_status", String, comment=""), #"开放/开放",
        # Column("amount_incr_tips", String, comment=""), #"最新份额：195万份；增长：0.33%",
        # Column("fund_nm_color", String, comment=""), #"香港本地",
        # Column("est_val_dt_s", String, comment=""), #"22-01-06",
        # Column("nav_dt_s", String, comment=""), #"22-01-05"
    ]

    def fetch(self):
        url = "https://www.jisilu.cn/data/qdii/qdii_list/A"+"?___jsl=LST___t={ctime:d}"

        dd = self.request(url, dict(
            rp = "50",
            page = "1"
        ))

        if len(dd) == 0:
            return dd

        dd = dd[["fund_id","last_est_datetime","discount_rt",
            "fund_nm","qtype","issuer_nm","asset_ratio","lof_type","index_nm",
            "price","price_dt","increase_rt",
            "volume","stock_volume","turnover_rt","amount","amount_incr","amount_increase_rt",
            "fund_nav","nav_dt",
            "last_est_dt","last_est_time","ref_price","ref_increase_rt","estimate_value","est_val_dt","est_val_increase_rt",]]

        # pd.to_datetime

        dd["discount_rt"] = dd["discount_rt"].apply(self.percentage2float)
        dd["increase_rt"] = dd["increase_rt"].apply(self.percentage2float)
        dd["turnover_rt"] = dd["turnover_rt"].apply(self.percentage2float)
        dd["amount_increase_rt"] = dd["amount_increase_rt"].apply(self.percentage2float)
        dd["ref_increase_rt"] = dd["ref_increase_rt"].apply(self.percentage2float)
        dd["est_val_increase_rt"] = dd["est_val_increase_rt"].apply(self.percentage2float)

        dd["asset_ratio"] = dd["asset_ratio"].apply(self.formatfloat)
        dd["price"] = dd["price"].apply(self.formatfloat)
        dd["volume"] = dd["volume"].apply(self.formatfloat)
        dd["stock_volume"] = dd["stock_volume"].apply(self.formatfloat)
        dd["fund_nav"] = dd["fund_nav"].apply(self.formatfloat)
        dd["ref_price"] = dd["ref_price"].apply(self.formatfloat)
        dd["estimate_value"] = dd["estimate_value"].apply(self.formatfloat)

        return dd

if __name__ == '__main__':
    # from database import tushare_api as api

    QDIIList = JSLQDIIList()

    # QDIIList.login()

    QDIIList.update()

    # https://www.jisilu.cn/webapi/cb/list/
    # ll = api.cb_basic()

    # import time

    # t = time.time()
    # for ticker in ll['ts_code'].values[::-1]:
    #     cbNewDetailHist.update(ticker = ticker[:6])

    #     if(time.time() - t > 5):
    #         logger.yellow('Wait 5s')
    #         time.sleep(5)
    #         t = time.time()