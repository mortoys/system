import numpy as np

from tool import *

# import os
# from os.path import join
# data_path = join(os.path.dirname(__file__), '../data')
# path = join(data_path, 'feature', 'logp')
# if os.path.exists(path): os.remove(path)
# path = join(data_path, 'feature', 'ret')
# if os.path.exists(path): os.remove(path)
# path = join(data_path, 'feature', 'mom')
# if os.path.exists(path): os.remove(path)

data = load_daily(['open', 'close', 'adj_factor'])

price = data['open'] * data['adj_factor']
price = np.log(price)
logp = price.to_frame('o_logp')

price = data['close'] * data['adj_factor']
logp['logp'] = np.log(price)

logp['mask'] = (data['open'] * data['adj_factor']).notna()

logp >> dump('feature/logp')

logp['o_logp_origin']  = logp['o_logp'].where(logp['mask'], np.nan)
logp['c_logp_origin'] = logp['logp'].where(logp['mask'], np.nan)
logp['o_logp_origin_nextday'] = logp['o_logp_origin'].shift(-1)
logp['c_logp_origin_lastday'] = logp['c_logp_origin'].shift(1)

logp >> group_by_asset(lambda s: dict(
    ret = s['logp'].diff(1).shift(-1),
    ret_5 = s['logp'].diff(5).shift(-5),
    ret_10 = s['logp'].diff(10).shift(-10),
    ret_22 = s['logp'].diff(22).shift(-22),
    ret_62 = s['logp'].diff(62).shift(-62),

    c_ret_overnight = s['o_logp_origin_nextday'] - s['c_logp_origin'],
    o_ret_intraday = s['c_logp_origin'] - s['o_logp_origin'],

)) >> qrank >> dump('feature/ret')

logp >> group_by_asset(lambda s: dict(
    mom_1 = s['logp'].diff(1),
    mom_5 = s['logp'].diff(5),
    mom_10 = s['logp'].diff(10),
    mom_22 = s['logp'].diff(22),
    mom_62 = s['logp'].diff(62),

    c_mom_intraday = s['c_logp_origin'] - s['o_logp_origin'],
    o_mom_overnight = s['o_logp_origin'] - s['c_logp_origin_lastday'],

)) >> qrank >> dump('feature/mom')
