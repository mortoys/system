
from tool import *

data = load_daily(['hk_vol', 'vol', 'close'])

def func(df):
    vol = df['vol'].ewm(halflife=15).mean()
    hk_vol = df['hk_vol'].diff().rolling(60).sum()
    # hk_vol = df['hk_vol'].diff().ewm(halflife=30).mean()

    vol_ln = np.log(vol)
    hk_vol_ln = np.log(hk_vol)
    return dict(
        HKCHG = hk_vol / vol,
        HKCHGLN = hk_vol_ln / vol_ln,
    )

data >> group_by_asset(func) >> qrank >> dump('feature/hk_hold')
