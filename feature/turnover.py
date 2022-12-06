import numpy as np

from tool import load_daily, group_by_asset, dump, qrank

data = load_daily(['turnover_rate_f'])

data >> group_by_asset(lambda s: dict(
    STOM = np.log(s['turnover_rate_f'].rolling(21).sum()),
    # 季换手率
    STOQ = np.log(s['turnover_rate_f'].rolling(63).sum()),
    # 年换手率
    STOA = np.log(s['turnover_rate_f'].rolling(252).sum()),
    # 对日交易份额比率(换手率)进行加权求和，时间窗口 252 个交易日，半衰期 63 个交易日
    ATVR = s['turnover_rate_f'].ewm(halflife=63).mean() )
) >> qrank >> dump('feature/turnover')
