import streamlit as st
import numpy as np
import pandas as pd
from database import query
from database import column, and_, or_
from datetime import date
from plotnine import ggplot, geom_point, aes, stat_smooth, facet_wrap

from .compo.stock_select import stock_select

def main():
    
    stock, stock_industry = stock_select()

    st.write(stock['industry'] + ' ' + stock['ts_code'] + ' ' + stock['name'])

    dd = query('tushare-balancesheet', cond=(column('ts_code') == stock['ts_code']))

    dd.index = dd['trade_date']

    # df[['fix_assets']]

    # df.index = df['trade_date']

    # st.line_chart(df['vol'])
    # st.line_chart(df['ratio'])


    # qq = query('tushare-hk_hold', cond=(column('ts_code').in_(stock_industry)))

    # df = qq[qq['ts_code'] == stock['ts_code']]

    # qq = qq.groupby('trade_date')[['vol', 'ratio']].mean()

    # df = df.merge(qq, on='trade_date', how='outer')

    # df.index = df['trade_date']

    # st.line_chart(df[['vol_x']])
    # st.line_chart(df[['ratio_x', 'ratio_y']])

    # st.area_chart(df[['total_cur_assets', 'total_nca']])

    

    notna_cols = ['total_ncl','fix_assets','acct_payable','accounts_receiv','intan_assets','inventories','prepayment',
        'payroll_payable','money_cap','surplus_rese','total_nca','total_cur_assets','total_cur_liab','taxes_payable',
        'cap_rese','undistr_porfit','total_share','total_hldr_eqy_inc_min_int','total_liab_hldr_eqy','total_liab',
        'total_hldr_eqy_exc_min_int','comp_type','total_assets']

    dd = dd.sort_values('end_date')

    dd[notna_cols] = dd[notna_cols].ffill()

    dd = dd.fillna(0)

    cur_assets = ['money_cap','trad_asset','notes_receiv','accounts_receiv','oth_receiv','prepayment','div_receiv','int_receiv',
        'inventories','amor_exp','nca_within_1y','sett_rsrv','loanto_oth_bank_fi','premium_receiv','reinsur_receiv',
        'reinsur_res_receiv','pur_resale_fa','oth_cur_assets',]

    cur_assets_name = ['货币资金','交易性金融资产','应收票据','应收账款','其他应收款','预付款项','应收股利','应收利息',
        '存货','待摊费用','一年内到期的非流动资产','结算备付金','拆出资金','应收保费','应收分保账款','应收分保合同准备金',
        '买入返售金融资产','其他流动资产']

    ncur_assets = ['fa_avail_for_sale','htm_invest','lt_eqt_invest','invest_real_estate','time_deposits','oth_assets','lt_rec',
        'fix_assets','cip','const_materials','fixed_assets_disp','produc_bio_assets','oil_and_gas_assets','intan_assets',
        'r_and_d','goodwill','lt_amor_exp','defer_tax_assets','decr_in_disbur','oth_nca',]
    ncur_assets_name = ['可供出售金融资产','持有至到期投资','长期股权投资','投资性房地产','定期存款','其他资产','长期应收款',
        '固定资产','在建工程','工程物资','固定资产清理','生产性生物资产','油气资产','无形资产',
        '研发支出','商誉','长期待摊费用','递延所得税资产','发放贷款及垫款','其他非流动资产']

    cur_liab = ['cb_borr','depos_ib_deposits','loan_oth_bank','trading_fl','notes_payable','acct_payable','adv_receipts','sold_for_repur_fa',
        'comm_payable','payroll_payable','taxes_payable','int_payable','div_payable','oth_payable','acc_exp','deferred_inc','st_bonds_payable',
        'payable_to_reinsurer','rsrv_insur_cont','acting_trading_sec','acting_uw_sec','non_cur_liab_due_1y','oth_cur_liab',]
    ncur_liab = ['bond_payable','lt_payable','specific_payables','estimated_liab','defer_tax_liab','defer_inc_non_cur_liab','oth_ncl',]
    liab = ['depos_oth_bfi','deriv_liab',
        'depos','agency_bus_liab','oth_liab','prem_receiv_adva','depos_received','ph_invest','reser_une_prem','reser_outstd_claims','reser_lins_liab',
        'reser_lthins_liab','indept_acc_liab','pledge_borr','indem_payable','policy_div_payable']
    
    cur_liab_name = ['向中央银行借款','吸收存款及同业存放','拆入资金','交易性金融负债','应付票据','应付账款','预收款项','卖出回购金融资产款','应付手续费及佣金',
        '应付职工薪酬','应交税费','应付利息','应付股利','其他应付款','预提费用','递延收益','应付短期债券','应付分保账款','保险合同准备金','代理买卖证券款','代理承销证券款',
        '一年内到期的非流动负债','其他流动负债']
    ncur_liab_name = ['应付债券','长期应付款','专项应付款','预计负债','递延所得税负债','递延收益-非流动负债','其他非流动负债']
    liab_name = ['同业和其它金融机构存放款项','衍生金融负债','吸收存款','代理业务负债','其他负债','预收保费','存入保证金',
        '保户储金及投资款','未到期责任准备金','未决赔款准备金','寿险责任准备金','长期健康险责任准备金','独立账户负债','其中:质押借款','应付赔付款','应付保单红利',]

    # st.area_chart(df[cur_assets].rename(dict(zip(cur_assets, cur_assets_name)), axis=1))
    # st.area_chart(df[ncur_assets].rename(dict(zip(ncur_assets, ncur_assets_name)), axis=1))

    data = dd[cur_assets + ncur_assets].rename(dict(zip(cur_assets + ncur_assets, cur_assets_name + ncur_assets_name)), axis=1)
    data2 = dd[cur_liab + ncur_liab].rename(dict(zip(cur_liab + ncur_liab, cur_liab_name + ncur_liab_name)), axis=1)

    df = data.merge(-data2, left_index=True, right_index=True)

    st.area_chart(df)

    rr = dd[['total_cur_assets', 'total_nca']].rename(dict(zip(['total_cur_assets', 'total_nca'], ['流动资产', '非流动资产'])), axis=1)
    st.area_chart((rr.T / rr.sum(axis=1)).T)

    st.area_chart((data.T / data.sum(axis=1)).T)

    rr = dd[['total_cur_liab', 'total_ncl']].rename(dict(zip(['total_cur_liab', 'total_ncl'], ['流动负债', '非流动负债'])), axis=1)
    st.area_chart((rr.T / rr.sum(axis=1)).T)
    
    st.area_chart((data2.T / data2.sum(axis=1)).T)

    rr = dd[['lt_borr', 'st_borr']].rename(dict(zip(['lt_borr', 'st_borr'], ['长期借款', '短期借款'])), axis=1)
    st.area_chart((rr.T / rr.sum(axis=1)).T)

    # st.write(data2.merge(-data, left_index=True, right_index=True))

    # st.area_chart((data.T / data.sum(axis=1)).T)
    
    # st.area_chart(df[ncur_assets].fillna(0).rename(dict(zip(ncur_assets, ncur_assets_name)), axis=1))

# total_cur_assets
# total_nca

#money_cap 货币资金
#trad_asset 交易性金融资产
#notes_receiv 应收票据
#accounts_receiv 应收账款
#oth_receiv 其他应收款
#prepayment 预付款项
#div_receiv 应收股利
#int_receiv 应收利息
#inventories 存货
#amor_exp 待摊费用
#nca_within_1y 一年内到期的非流动资产
#sett_rsrv 结算备付金
#loanto_oth_bank_fi 拆出资金
#premium_receiv 应收保费
#reinsur_receiv 应收分保账款
#reinsur_res_receiv 应收分保合同准备金
#pur_resale_fa 买入返售金融资产
#oth_cur_assets 其他流动资产
#total_cur_assets 流动资产合计

#fa_avail_for_sale 可供出售金融资产
#htm_invest 持有至到期投资
#lt_eqt_invest 长期股权投资
#invest_real_estate 投资性房地产
#time_deposits 定期存款
#oth_assets 其他资产
#lt_rec 长期应收款
#fix_assets 固定资产
#cip 在建工程
#const_materials 工程物资
#fixed_assets_disp 固定资产清理
#produc_bio_assets 生产性生物资产
#oil_and_gas_assets 油气资产
#intan_assets 无形资产
#r_and_d 研发支出
#goodwill 商誉
#lt_amor_exp 长期待摊费用
#defer_tax_assets 递延所得税资产
#decr_in_disbur 发放贷款及垫款
#oth_nca 其他非流动资产
#total_nca 非流动资产合计