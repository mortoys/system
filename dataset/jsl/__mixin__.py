import json
import time
import pandas as pd
import numpy as np
import requests

from database.local import jsl_username_encode, jsl_password_encode
from database import logger
class JSLMixin:
    domain = "jsl"

    __jsl_login_url = "https://www.jisilu.cn/account/ajax/login_process/"

    session = None

    def init(self):
        self.session = requests.session()

        headers = {
            "Origin": "https://www.jisilu.cn",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.jisilu.cn/data/cbnew/",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        }
        self.session.headers.update(headers)

    def login(self):
        if self.session is None:
            self.init()

        logindata = dict(
            return_url = "http://www.jisilu.cn/",
            user_name = jsl_username_encode,
            password = jsl_password_encode,
            net_auto_login = "1",
            _post_type = "ajax",
            aes = 1
        )

        rep = self.session.post(self.__jsl_login_url, data = logindata)

        if rep.json()["err"] is not None:
            return rep.json()

    def percentage2float(self, per):
        "将字符串的百分数转化为浮点数"
        if per.endswith("%"):
            return float(per.strip("%")) / 100.
        else:
            return np.nan

    def formatfloat(self, value):
        if value is None or value == '-':
            return np.nan
        else:
            return float(value)

    def formatjson(self, data):
        if 'rows' not in data or len(data['rows']) == 0:
            return pd.DataFrame()

        return pd.DataFrame(map(lambda s: s['cell'], data['rows']))

    def request(self, url, params):
        if self.session is None:
            self.init()

        __url = url.format(ctime=int(time.time()))

        logger.green('URL: ' + __url)

        rep = self.session.post(__url, data = params)

        reqjson = json.loads(rep.text)

        data = self.formatjson(reqjson)

        return data
