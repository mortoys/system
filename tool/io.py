import os
from os.path import join

import pandas as pd
from plydata import call, select, rename

data_path = join(os.path.dirname(__file__), '../data')

def dump(path, index=True):
    return call('.to_parquet', 
        path = join(data_path, path),
        engine = 'pyarrow',
        compression = 'snappy',
        index = index
    )

def load(path, columns=None):
    return pd.read_parquet(
        join(data_path, path), columns = columns)

def load_daily(columns):
    return load('clean/astock-daily', columns = columns)

def load_feature(path, columns=None):
    return load(join('feature', path), columns = columns)

from functools import reduce
def merge(dfs, how='inner'):
    return reduce(lambda x, y: pd.merge(x, y,
        left_index=True, right_index=True,
        how = how
    ), dfs)

ret_merge = call(lambda df: merge(df, 
    load_feature('ret', ['ret_1', 'ret_5', 'ret_10', 'ret_22', 'ret_62'])))

def load_merge_return(path, columns=None):
    return load(path, columns=columns) >> ret_merge

# def qunatile(factor_col):
#     def q(s):
#         return pd.DataFrame(dict(
#             factor = s,
#             qrank = s.rank(pct=True),
#             # quantile = quantile_transform(s.to_numpy().reshape(-1,1), n_quantiles=1000).reshape(-1),
#             s3 = pd.qcut(s, 3, labels=['A', 'M', 'Z'], duplicates='drop').astype('category'),
#             s5 = pd.qcut(s, 5, labels=['A', 'B', 'M', 'Y', 'Z'], duplicates='drop').astype('category'),
#             s1_2 = pd.qcut(s,  [0, .15, .85, 1.], labels=['A', 'M', 'Z'], duplicates='drop').astype('category'),
#             s5_2 = pd.qcut(s,  [0, .1, .26, .42, .58, .74, .9, 1.], labels=['A', 'B', 'C', 'M', 'X', 'Y', 'Z'], duplicates='drop').astype('category'),
#         ))

#     return call(lambda df: df[factor_col].dropna().groupby('date').progress_apply(q))

# group_by_date(lambda s: s.rank(pct=True))

# def size_qunatile(env):
#     return (load_daily(['circ_mv'])
#         >> env()
#         >> qunatile('circ_mv')
#         >> select('s3')
#         >> rename(_s3='s3'))

# def size_qunatile(env):
#     return load('factor/size_' + env)

# def assemble(fname, name, env, base=size_qunatile):
#     feature = load_feature(fname) >> env()
#     factor = merge(
#         feature >> qunatile(name) >> ret_merge,
#         size_qunatile(env))
#     return factor

def compute_index(env):
    logp = load_feature('logp', ['logp'])
    circ_mv = load_daily(['circ_mv'])
    data = merge(logp, circ_mv)

    dd = data >> env()
    dd['ratio'] = dd['circ_mv'] / dd['circ_mv'].unstack().sum(axis=1)
    return (dd['logp'] * dd['ratio']).unstack().sum(axis=1).to_frame('index_logp')

def load_index(name):
    return load(join('index', name))