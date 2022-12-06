from tool import *

# data = load('clean/astock-season').drop(['update_flag'], axis=1)

close = load_daily(['close'])

# data = data >> group_by_asset(lambda s: s.ffill())

# data = (data.reset_index()
#     .rename(dict(end_date='date', date='update_date'), axis=1)
#     .set_index(['date','asset'])
# )

# data = pd.concat([
#     data.iloc[:,1:] >> qrank_just,
#     data.iloc[:,[0]]
# ])

# data = (data.reset_index()
#     .rename(dict(date='end_date', update_date='date'), axis=1)
#     .set_index(['date','asset'])
# )

# data = merge([
#     close,
#     data,
# ], how='outer').drop(['close'], axis=1) >> group_by_asset(lambda s: s.ffill()) >> dump('feature/fina_indicator')

data = load('feature/fina_indicator')

merge([
    close,
    data,
], how='left').drop(['close'], axis=1) >> dump('feature/fina_indicator2')

# revenue_ps 每股营业收入
# gross_margin 毛利
# tr_yoy 营业总收入同比增长率(%)
# netdebt 净债务
# adminexp_of_gr 管理费用/营业总收入
# undist_profit_ps 每股未分配利润
# surplus_rese_ps 每股盈余公积
# roe_waa 加权平均净资产收益率
# equity_yoy 净资产同比增长率

# dd['undist_profit_ps'] - dd['surplus_rese_ps']