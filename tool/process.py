import pandas as pd
import numpy as np
from plydata import call, select, rename
from tqdm.autonotebook import tqdm
tqdm.pandas()

from tool.subset import *
from tool.io import merge

def group_by_date(func):
    return call(lambda df: df
        .groupby('date')
        .progress_apply(lambda s: pd.DataFrame(func(s), index = s.index)))

def group_by_asset(func):
    return call(lambda df: df
        .groupby('asset')
        .progress_apply(lambda s: pd.DataFrame(func(s), index = s.index)))

def ret_mean(group=None, base=None, ret='ret_1', func=np.nanmean, neutral=True):
    if base is not None and group is not None:
        groupby = [pd.Grouper(base), pd.Grouper(group)]
        names = ['base', 'group']
    elif group is not None:
        groupby = [pd.Grouper(group)]
        names = ['group']
    else:
        groupby = []
        names = None
    
    def process(df):
        dd = (df
            .groupby(['date'] + groupby)[ret]
            .apply(func)
            .reset_index().set_index('date')
            .pivot(columns=[g.key for g in groupby]).fillna(0)[ret])
        if names is not None:
            dd.columns = dd.columns.set_names(names)
            return dd
        else:
            return dd.to_frame('ret')
    
    return call(process)

ret_neutral = call(lambda df:
    (df.T - df.T.groupby('base').mean()).T
    if 'base' in df.columns.names
    else (df.T - df.mean(axis=1)).T
)

define_year = call(lambda df: df.assign(year=df.index.get_level_values('date').year))

define_ym = call(lambda df: df.assign(year=df.index.get_level_values('date').strftime('%Y-%m')))


from scipy.stats import spearmanr

def ic(period):
    def func(df):
        ic_data = (df[['qrank', period]].groupby('date')
            .apply(lambda s: spearmanr(s[period], s['qrank'], nan_policy='omit')[0]))
        return (ic_data.rolling(30).mean()).to_frame('ic '+period).reset_index()

    return call(func)

def ir(period):

    def func(df):
        ic_data = (df[['qrank', period]].groupby('date')
            .apply(lambda s: spearmanr(s[period], s['qrank'], nan_policy='omit')[0]))
        return (ic_data.rolling(30).mean() / ic_data.rolling(30).std()).to_frame('ir '+period).reset_index()

    return call(func)

def after(date):
    return call(lambda data: data[data.index.get_level_values('date') > date])

def to_SHSZ(code):
    """ 
    尾部加 SH SZ
    深交所 XSHE SZ 0 3 1ETF
    上交所 XSHG SH  6 9 5ETF
    """
    code = '{:0>6d}'.format(int(code))
    tail = 'Z' if code[0] in '013' else 'H'
    return code[:6] + '.S' + tail

def head_SHSZ(code):
    """ 
    头部加 SH SZ 
    深交所 XSHE SZ 0 3 1ETF
    上交所 XSHG SH  6 9 5ETF
    """
    code = '{:0>6d}'.format(int(code))
    tail = 'Z' if code[0] in '013' else 'H'
    return 'S' + tail + code[:6]

def to_XSH(code):
    """ 变成 XSHG XSHE """
    # '0E', '3E', '6G', '9G'
    code = '{:0>6d}'.format(int(code))
    tail = 'E' if(code[0] == '0' or code[0] == '3') else 'G'
    return code[:6] + '.XSH' + tail

def Symmetry(factors):
    D, U = np.linalg.eig(np.dot(factors.T,factors))
    S = np.dot(U,np.diag(D**(-0.5)))

    Fhat = np.dot(factors,S)
    Fhat = np.dot(Fhat,U.T)
    Fhat = pd.DataFrame(Fhat, columns = factors.columns, index = factors.index)

    return Fhat

from statsmodels.tsa.filters.hp_filter import hpfilter
def hp(lamb=5e3):
    return call(lambda df: df.apply(lambda s: hpfilter(s, lamb)[1]))

standardize = call(lambda dt: (dt - dt.mean()) / dt.std().apply(lambda s: np.nanmax([s, 0.01])))

from sklearn.linear_model import LinearRegression, Ridge, Lasso
def linear_func(df):
    y = df.iloc[:,0]
    X = (df.iloc[:,1:] >> standardize)

    reg = Ridge(alpha=0.0001)
    reg.fit(X, y)

    return pd.Series(reg.coef_, index=X.columns)

linear = call(lambda df: df.dropna().groupby('date').apply(linear_func))

linear_group = call(lambda df: df.dropna().groupby(['date', pd.Grouper('group')]).apply(linear_func).drop('group', axis=1))

def qrank_just_func(df):
    return pd.concat([
        df >> in_main() >> group_by_date(lambda s: s.rank(pct=True)),
        df >> in_kechuang() >> group_by_date(lambda s: s.rank(pct=True)),
        df >> in_zhongxiao() >> group_by_date(lambda s: s.rank(pct=True)),
        df >> in_chuangye() >> group_by_date(lambda s: s.rank(pct=True)),
    ]).sort_index()

def qrank_func(df):
    data = qrank_just_func(df)
    return merge([df,
        data.rename(columns={ col: 'r_'+col for col in data.columns}) ])

qrank = call(qrank_func)

qrank_just = call(qrank_just_func)