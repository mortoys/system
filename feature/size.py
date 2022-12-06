import pandas as pd
import numpy as np

from scipy.stats import linregress
from scipy.stats.mstats import winsorize

from tool import *

data = load_daily(['circ_mv'])

LNCAP = np.log(data['circ_mv'])

def compute(lnmv):
    X = lnmv
    Y = X**3
    mask = ~np.isnan(X) & ~np.isnan(Y)
    if len(X[mask]) == 0:
        factor = pd.Series(0, index=X.index)
    else:
        (beta, alpha, rvalue) = linregress(X[mask].to_numpy(), Y[mask].to_numpy())[0:3]
        factor = Y - beta*X - alpha
        winsorized = winsorize(factor, limits=[0.005, 0.005])
        factor = pd.Series(winsorized, index=factor.index)
        factor = (factor - factor.mean()) / factor.std()
    return factor

MIDCAP = LNCAP.groupby(level='date').apply(compute)

pd.DataFrame(dict(
    # 流通市值的自然对数
    LNCAP = LNCAP,
    # 首先取 Size 因子暴露的立方，然后以加权回归的方式对 Size 因子正交，最后进行去极值和标准化处理
    # https://zhuanlan.zhihu.com/p/150310851
    MIDCAP = MIDCAP,
), index = data.index) >> qrank >> dump('feature/size')
