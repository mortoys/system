import pandas as pd
import numpy as np

from scipy.stats import linregress
# from scipy.stats.mstats import winsorize

from tool import load_index, load_feature, group_by_asset, dump, merge, qrank

def compute(row):
    index_r1 = row['index_logp'].diff()

    return_1 = row['mom_1'].fillna(0)

    ret = pd.DataFrame(dict(beta=np.nan, alpha=np.nan, rvalue=np.nan), index=return_1.index)

    for i in range(250, len(row), 10):
        Y = return_1[i-250: i].to_numpy()
        X = index_r1[i-250: i].to_numpy()

        if(Y[0] == None or np.isnan(Y[0])):
            (beta, alpha, rvalue) = (np.nan, np.nan, np.nan)
        else:
            (beta, alpha, rvalue) = linregress(X, Y)[0:3]
            ret.iloc[i] = (beta, alpha, rvalue)

    ret = ret.interpolate()

    HALPHA = ret['alpha']

    HSIGMA = ret['alpha'].rolling(22).std()

    DASTD = return_1.ewm(halflife=42).std()

    return pd.DataFrame(dict(
        # 股票收益率 对沪深 300 收益率 进行时间序列回归，取回归 系数，回归时间窗口为 252 个交易日，半衰期 63 个交易日
        BETA = ret['beta'],
        # 这是一个动量因子
        # 在计算 BETA 所进行的时间序列回归中，取回归截距项
        HALPHA = HALPHA,
        # 在计算 BETA 所进行的时间序列回归中，取回归残差收益率的波动率
        HSIGMA = HSIGMA,
        # 日收益率在过去 252 个交易日的波动率，半衰期 42 个交易日
        DASTD = DASTD,
        # 为过去 T 个月累积对数收益率(每个月包含 21 个交易日) 最大值减最小值
        # CMRA = 
    ), index=return_1.index)

from tool import in_main, in_kechuang, in_chuangye, in_zhongxiao

pd.concat([
    merge([
        load_feature('mom', ['mom_1']) >> in_main(),
        load_index('main')
    ]) >> group_by_asset(compute),
    merge([
        load_feature('mom', ['mom_1']) >> in_kechuang(),
        load_index('kechuang')
    ]) >> group_by_asset(compute),
    merge([
        load_feature('mom', ['mom_1']) >> in_chuangye(),
        load_index('chuangye')
    ]) >> group_by_asset(compute),
    merge([
        load_feature('mom', ['mom_1']) >> in_zhongxiao(),
        load_index('zhongxiao')
    ]) >> group_by_asset(compute),
]) >> qrank >> dump('feature/volatility')

# from tool import plot

# plot('volatility', 'BETA', 'main', in_main)
# plot('volatility', 'BETA', 'kechuang', in_kechuang)
# plot('volatility', 'BETA', 'zhongxiao', in_zhongxiao)
# plot('volatility', 'BETA', 'chuangye', in_chuangye)

# plot('volatility', 'HALPHA', 'main', in_main)
# plot('volatility', 'HALPHA', 'kechuang', in_kechuang)
# plot('volatility', 'HALPHA', 'zhongxiao', in_zhongxiao)
# plot('volatility', 'HALPHA', 'chuangye', in_chuangye)

# plot('volatility', 'HSIGMA', 'main', in_main)
# plot('volatility', 'HSIGMA', 'kechuang', in_kechuang)
# plot('volatility', 'HSIGMA', 'zhongxiao', in_zhongxiao)
# plot('volatility', 'HSIGMA', 'chuangye', in_chuangye)

# plot('volatility', 'DASTD', 'main', in_main)
# plot('volatility', 'DASTD', 'kechuang', in_kechuang)
# plot('volatility', 'DASTD', 'zhongxiao', in_zhongxiao)
# plot('volatility', 'DASTD', 'chuangye', in_chuangye)