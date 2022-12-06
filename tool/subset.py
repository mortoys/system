import pandas as pd
from database import tushare_api as api
from plydata import call, select, rename
from functools import lru_cache
from tool.io import load

def in_tickers(tickers):
    return call(lambda df: df[df.index.get_level_values('asset').isin(tickers)])

@lru_cache
def get_block():
    df = api.stock_basic()
    data = df[['ts_code','market']].set_index('ts_code')
    data['market'] = data['market'].astype('category')
    return data['market']
def in_kechuang():
    data = get_block()
    return in_tickers(data[data == '科创板'].index)
def in_zhongxiao():
    data = get_block()
    return in_tickers(data[data == '中小板'].index)
def in_chuangye():
    data = get_block()
    return in_tickers(data[data == '创业板'].index)
def in_main():
    data = get_block()
    return in_tickers(data[data == '主板'].index)
def in_cdr():
    data = get_block()
    return in_tickers(data[data == 'CDR'].index)


from tool.io import load


@lru_cache
def get_industry():
    df = load('backup/opendata/industry_cons')
    df[['index_code','index_name','ts_code','name']] = df[['index_code','index_name','ts_code','name']].astype('category')
    dd = df[['index_code', 'index_name', 'weight']]
    dd.index = df['ts_code']
    dd.index.name = 'asset'
    dd = dd.rename(dict(index_code='classify_code', index_name='classify_name'), axis=1)

    return dd
def classify_industry():
    data = get_industry()
    return pd.get_dummies(data['classify_name'], prefix='', prefix_sep='')
