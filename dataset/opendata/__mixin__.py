# from database import tushare_api
# from datetime import datetime, timedelta
# from dataset.type import DatasetDailyTicker, DatasetDailyIndex, DatasetDaily, DatasetTicker
import numpy as np
# import opendatatools

class OpenDataMixin:
    domain = "opendata"

    @property
    def od_date(self):
        return self.date.strftime("%Y-%m-%d")

    def percentage2float(self, per):
        "将字符串的百分数转化为浮点数"
        if per.endswith("%"):
            return float(per.strip("%")) / 100.
        else:
            return np.nan

    def formatfloat(self, value):
        if value is None or value == '' or value == '-':
            return np.nan
        else:
            return float(value.replace(',',''))

    def to_SHSZ(self, code):
        """ 
        尾部加 SH SZ
        深交所 XSHE SZ 0 3 1ETF
        上交所 XSHG SH  6 9 5ETF
        """
        code = '{:0>6d}'.format(int(code))
        tail = 'Z' if code[0] in '013' else 'H'
        return code[:6] + '.S' + tail
