import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from database import logger
from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyUpdate

cols = [
    Column("ts_code", String, comment="TS股票代码", primary_key=True),
    Column("ann_date", Date, comment="公告日期"),
    Column("trade_date", Date, comment="实际公告日期", primary_key=True),
    Column("end_date", Date, comment="报告期"),
    Column("report_type", String, comment="报表类型", primary_key=True),
    Column("comp_type", String, comment="公司类型"),
    Column("total_share", Float, comment="期末总股本"),
    Column("cap_rese", Float, comment="资本公积金"),
    Column("undistr_porfit", Float, comment="未分配利润"),
    Column("surplus_rese", Float, comment="盈余公积金"),
    Column("special_rese", Float, comment="专项储备"),
    Column("money_cap", Float, comment="货币资金"),
    Column("trad_asset", Float, comment="交易性金融资产"),
    Column("notes_receiv", Float, comment="应收票据"),
    Column("accounts_receiv", Float, comment="应收账款"),
    Column("oth_receiv", Float, comment="其他应收款"),
    Column("prepayment", Float, comment="预付款项"),
    Column("div_receiv", Float, comment="应收股利"),
    Column("int_receiv", Float, comment="应收利息"),
    Column("inventories", Float, comment="存货"),
    Column("amor_exp", Float, comment="待摊费用"),
    Column("nca_within_1y", Float, comment="一年内到期的非流动资产"),
    Column("sett_rsrv", Float, comment="结算备付金"),
    Column("loanto_oth_bank_fi", Float, comment="拆出资金"),
    Column("premium_receiv", Float, comment="应收保费"),
    Column("reinsur_receiv", Float, comment="应收分保账款"),
    Column("reinsur_res_receiv", Float, comment="应收分保合同准备金"),
    Column("pur_resale_fa", Float, comment="买入返售金融资产"),
    Column("oth_cur_assets", Float, comment="其他流动资产"),
    Column("total_cur_assets", Float, comment="流动资产合计"),
    Column("fa_avail_for_sale", Float, comment="可供出售金融资产"),
    Column("htm_invest", Float, comment="持有至到期投资"),
    Column("lt_eqt_invest", Float, comment="长期股权投资"),
    Column("invest_real_estate", Float, comment="投资性房地产"),
    Column("time_deposits", Float, comment="定期存款"),
    Column("oth_assets", Float, comment="其他资产"),
    Column("lt_rec", Float, comment="长期应收款"),
    Column("fix_assets", Float, comment="固定资产"),
    Column("cip", Float, comment="在建工程"),
    Column("const_materials", Float, comment="工程物资"),
    Column("fixed_assets_disp", Float, comment="固定资产清理"),
    Column("produc_bio_assets", Float, comment="生产性生物资产"),
    Column("oil_and_gas_assets", Float, comment="油气资产"),
    Column("intan_assets", Float, comment="无形资产"),
    Column("r_and_d", Float, comment="研发支出"),
    Column("goodwill", Float, comment="商誉"),
    Column("lt_amor_exp", Float, comment="长期待摊费用"),
    Column("defer_tax_assets", Float, comment="递延所得税资产"),
    Column("decr_in_disbur", Float, comment="发放贷款及垫款"),
    Column("oth_nca", Float, comment="其他非流动资产"),
    Column("total_nca", Float, comment="非流动资产合计"),
    Column("cash_reser_cb", Float, comment="现金及存放中央银行款项"),
    Column("depos_in_oth_bfi", Float, comment="存放同业和其它金融机构款项"),
    Column("prec_metals", Float, comment="贵金属"),
    Column("deriv_assets", Float, comment="衍生金融资产"),
    Column("rr_reins_une_prem", Float, comment="应收分保未到期责任准备金"),
    Column("rr_reins_outstd_cla", Float, comment="应收分保未决赔款准备金"),
    Column("rr_reins_lins_liab", Float, comment="应收分保寿险责任准备金"),
    Column("rr_reins_lthins_liab", Float, comment="应收分保长期健康险责任准备金"),
    Column("refund_depos", Float, comment="存出保证金"),
    Column("ph_pledge_loans", Float, comment="保户质押贷款"),
    Column("refund_cap_depos", Float, comment="存出资本保证金"),
    Column("indep_acct_assets", Float, comment="独立账户资产"),
    Column("client_depos", Float, comment="其中：客户资金存款"),
    Column("client_prov", Float, comment="其中：客户备付金"),
    Column("transac_seat_fee", Float, comment="其中:交易席位费"),
    Column("invest_as_receiv", Float, comment="应收款项类投资"),
    Column("total_assets", Float, comment="资产总计"),
    Column("lt_borr", Float, comment="长期借款"),
    Column("st_borr", Float, comment="短期借款"),
    Column("cb_borr", Float, comment="向中央银行借款"),
    Column("depos_ib_deposits", Float, comment="吸收存款及同业存放"),
    Column("loan_oth_bank", Float, comment="拆入资金"),
    Column("trading_fl", Float, comment="交易性金融负债"),
    Column("notes_payable", Float, comment="应付票据"),
    Column("acct_payable", Float, comment="应付账款"),
    Column("adv_receipts", Float, comment="预收款项"),
    Column("sold_for_repur_fa", Float, comment="卖出回购金融资产款"),
    Column("comm_payable", Float, comment="应付手续费及佣金"),
    Column("payroll_payable", Float, comment="应付职工薪酬"),
    Column("taxes_payable", Float, comment="应交税费"),
    Column("int_payable", Float, comment="应付利息"),
    Column("div_payable", Float, comment="应付股利"),
    Column("oth_payable", Float, comment="其他应付款"),
    Column("acc_exp", Float, comment="预提费用"),
    Column("deferred_inc", Float, comment="递延收益"),
    Column("st_bonds_payable", Float, comment="应付短期债券"),
    Column("payable_to_reinsurer", Float, comment="应付分保账款"),
    Column("rsrv_insur_cont", Float, comment="保险合同准备金"),
    Column("acting_trading_sec", Float, comment="代理买卖证券款"),
    Column("acting_uw_sec", Float, comment="代理承销证券款"),
    Column("non_cur_liab_due_1y", Float, comment="一年内到期的非流动负债"),
    Column("oth_cur_liab", Float, comment="其他流动负债"),
    Column("total_cur_liab", Float, comment="流动负债合计"),
    Column("bond_payable", Float, comment="应付债券"),
    Column("lt_payable", Float, comment="长期应付款"),
    Column("specific_payables", Float, comment="专项应付款"),
    Column("estimated_liab", Float, comment="预计负债"),
    Column("defer_tax_liab", Float, comment="递延所得税负债"),
    Column("defer_inc_non_cur_liab", Float, comment="递延收益-非流动负债"),
    Column("oth_ncl", Float, comment="其他非流动负债"),
    Column("total_ncl", Float, comment="非流动负债合计"),
    Column("depos_oth_bfi", Float, comment="同业和其它金融机构存放款项"),
    Column("deriv_liab", Float, comment="衍生金融负债"),
    Column("depos", Float, comment="吸收存款"),
    Column("agency_bus_liab", Float, comment="代理业务负债"),
    Column("oth_liab", Float, comment="其他负债"),
    Column("prem_receiv_adva", Float, comment="预收保费"),
    Column("depos_received", Float, comment="存入保证金"),
    Column("ph_invest", Float, comment="保户储金及投资款"),
    Column("reser_une_prem", Float, comment="未到期责任准备金"),
    Column("reser_outstd_claims", Float, comment="未决赔款准备金"),
    Column("reser_lins_liab", Float, comment="寿险责任准备金"),
    Column("reser_lthins_liab", Float, comment="长期健康险责任准备金"),
    Column("indept_acc_liab", Float, comment="独立账户负债"),
    Column("pledge_borr", Float, comment="其中:质押借款"),
    Column("indem_payable", Float, comment="应付赔付款"),
    Column("policy_div_payable", Float, comment="应付保单红利"),
    Column("total_liab", Float, comment="负债合计"),
    Column("treasury_share", Float, comment="减:库存股"),
    Column("ordin_risk_reser", Float, comment="一般风险准备"),
    Column("forex_differ", Float, comment="外币报表折算差额"),
    Column("invest_loss_unconf", Float, comment="未确认的投资损失"),
    Column("minority_int", Float, comment="少数股东权益"),
    Column("total_hldr_eqy_exc_min_int", Float, comment="股东权益合计(不含少数股东权益)"),
    Column("total_hldr_eqy_inc_min_int", Float, comment="股东权益合计(含少数股东权益)"),
    Column("total_liab_hldr_eqy", Float, comment="负债及股东权益总计"),
    Column("lt_payroll_payable", Float, comment="长期应付职工薪酬"),
    Column("oth_comp_income", Float, comment="其他综合收益"),
    Column("oth_eqt_tools", Float, comment="其他权益工具"),
    Column("oth_eqt_tools_p_shr", Float, comment="其他权益工具(优先股)"),
    Column("lending_funds", Float, comment="融出资金"),
    Column("acc_receivable", Float, comment="应收款项"),
    Column("st_fin_payable", Float, comment="应付短期融资款"),
    Column("payables", Float, comment="应付款项"),
    Column("hfs_assets", Float, comment="持有待售的资产"),
    Column("hfs_sales", Float, comment="持有待售的负债"),
    Column("update_flag", String, comment="更新标识，0未修改1更正过")
]

class TushareBalancesheet(DailyUpdate, TushareMixin):
    """资产负债表"""

    name = "tushare-balancesheet"

    schedule_start_date = (date.today() - timedelta(days=1)).isoformat()
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    columns = cols

    def fetch(self):
        period = self.params['period'] if 'period' in self.params else self.current_period()
        logger.yellow('PERIOD: ' + period)

        data = self.api.balancesheet_vip(period=period)
        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data['f_ann_date'] = data['f_ann_date'].fillna(data['ann_date'])
        data = data.rename(columns=dict(f_ann_date = 'trade_date'))
        data = data[[col.name for col in self.columns]]
        data = data.drop_duplicates(["ts_code", "trade_date", "report_type"], keep='last')
        data = data.set_index(["ts_code", "trade_date", "report_type"])
        return data

if __name__ == '__main__':
    updater = TushareBalancesheet()

    years = list(range(1990, 2022))
    quarters = ['0331', '0630', '0930', '1231']
    data = pd.DataFrame()

    # for year in years:
    #     for quarter in quarters:
    #         period = str(year) + quarter
    for period in ['20210930','20211231','20220331','20220630','20220930','20221231',]:
        updater.update(date=date.today().isoformat(), period=period)
        while updater.status == 'error':
            updater.update(date=date.today().isoformat(), period=period)