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
    Column("comp_type", String, comment="公司类型"),
    Column("report_type", String, comment="报表类型", primary_key=True),
    Column("net_profit", Float, comment="净利润"),
    Column("finan_exp", Float, comment="财务费用"),
    Column("c_fr_sale_sg", Float, comment="销售商品、提供劳务收到的现金"),
    Column("recp_tax_rends", Float, comment="收到的税费返还"),
    Column("n_depos_incr_fi", Float, comment="客户存款和同业存放款项净增加额"),
    Column("n_incr_loans_cb", Float, comment="向中央银行借款净增加额"),
    Column("n_inc_borr_oth_fi", Float, comment="向其他金融机构拆入资金净增加额"),
    Column("prem_fr_orig_contr", Float, comment="收到原保险合同保费取得的现金"),
    Column("n_incr_insured_dep", Float, comment="保户储金净增加额"),
    Column("n_reinsur_prem", Float, comment="收到再保业务现金净额"),
    Column("n_incr_disp_tfa", Float, comment="处置交易性金融资产净增加额"),
    Column("ifc_cash_incr", Float, comment="收取利息和手续费净增加额"),
    Column("n_incr_disp_faas", Float, comment="处置可供出售金融资产净增加额"),
    Column("n_incr_loans_oth_bank", Float, comment="拆入资金净增加额"),
    Column("n_cap_incr_repur", Float, comment="回购业务资金净增加额"),
    Column("c_fr_oth_operate_a", Float, comment="收到其他与经营活动有关的现金"),
    Column("c_inf_fr_operate_a", Float, comment="经营活动现金流入小计"),
    Column("c_paid_goods_s", Float, comment="购买商品、接受劳务支付的现金"),
    Column("c_paid_to_for_empl", Float, comment="支付给职工以及为职工支付的现金"),
    Column("c_paid_for_taxes", Float, comment="支付的各项税费"),
    Column("n_incr_clt_loan_adv", Float, comment="客户贷款及垫款净增加额"),
    Column("n_incr_dep_cbob", Float, comment="存放央行和同业款项净增加额"),
    Column("c_pay_claims_orig_inco", Float, comment="支付原保险合同赔付款项的现金"),
    Column("pay_handling_chrg", Float, comment="支付手续费的现金"),
    Column("pay_comm_insur_plcy", Float, comment="支付保单红利的现金"),
    Column("oth_cash_pay_oper_act", Float, comment="支付其他与经营活动有关的现金"),
    Column("st_cash_out_act", Float, comment="经营活动现金流出小计"),
    Column("n_cashflow_act", Float, comment="经营活动产生的现金流量净额"),
    Column("oth_recp_ral_inv_act", Float, comment="收到其他与投资活动有关的现金"),
    Column("c_disp_withdrwl_invest", Float, comment="收回投资收到的现金"),
    Column("c_recp_return_invest", Float, comment="取得投资收益收到的现金"),
    Column("n_recp_disp_fiolta", Float, comment="处置固定资产、无形资产和其他长期资产收回的现金净额"),
    Column("n_recp_disp_sobu", Float, comment="处置子公司及其他营业单位收到的现金净额"),
    Column("stot_inflows_inv_act", Float, comment="投资活动现金流入小计"),
    Column("c_pay_acq_const_fiolta", Float, comment="购建固定资产、无形资产和其他长期资产支付的现金"),
    Column("c_paid_invest", Float, comment="投资支付的现金"),
    Column("n_disp_subs_oth_biz", Float, comment="取得子公司及其他营业单位支付的现金净额"),
    Column("oth_pay_ral_inv_act", Float, comment="支付其他与投资活动有关的现金"),
    Column("n_incr_pledge_loan", Float, comment="质押贷款净增加额"),
    Column("stot_out_inv_act", Float, comment="投资活动现金流出小计"),
    Column("n_cashflow_inv_act", Float, comment="投资活动产生的现金流量净额"),
    Column("c_recp_borrow", Float, comment="取得借款收到的现金"),
    Column("proc_issue_bonds", Float, comment="发行债券收到的现金"),
    Column("oth_cash_recp_ral_fnc_act", Float, comment="收到其他与筹资活动有关的现金"),
    Column("stot_cash_in_fnc_act", Float, comment="筹资活动现金流入小计"),
    Column("free_cashflow", Float, comment="企业自由现金流量"),
    Column("c_prepay_amt_borr", Float, comment="偿还债务支付的现金"),
    Column("c_pay_dist_dpcp_int_exp", Float, comment="分配股利、利润或偿付利息支付的现金"),
    Column("incl_dvd_profit_paid_sc_ms", Float, comment="其中:子公司支付给少数股东的股利、利润"),
    Column("oth_cashpay_ral_fnc_act", Float, comment="支付其他与筹资活动有关的现金"),
    Column("stot_cashout_fnc_act", Float, comment="筹资活动现金流出小计"),
    Column("n_cash_flows_fnc_act", Float, comment="筹资活动产生的现金流量净额"),
    Column("eff_fx_flu_cash", Float, comment="汇率变动对现金的影响"),
    Column("n_incr_cash_cash_equ", Float, comment="现金及现金等价物净增加额"),
    Column("c_cash_equ_beg_period", Float, comment="期初现金及现金等价物余额"),
    Column("c_cash_equ_end_period", Float, comment="期末现金及现金等价物余额"),
    Column("c_recp_cap_contrib", Float, comment="吸收投资收到的现金"),
    Column("incl_cash_rec_saims", Float, comment="其中:子公司吸收少数股东投资收到的现金"),
    Column("uncon_invest_loss", Float, comment="未确认投资损失"),
    Column("prov_depr_assets", Float, comment="加:资产减值准备"),
    Column("depr_fa_coga_dpba", Float, comment="固定资产折旧、油气资产折耗、生产性生物资产折旧"),
    Column("amort_intang_assets", Float, comment="无形资产摊销"),
    Column("lt_amort_deferred_exp", Float, comment="长期待摊费用摊销"),
    Column("decr_deferred_exp", Float, comment="待摊费用减少"),
    Column("incr_acc_exp", Float, comment="预提费用增加"),
    Column("loss_disp_fiolta", Float, comment="处置固定、无形资产和其他长期资产的损失"),
    Column("loss_scr_fa", Float, comment="固定资产报废损失"),
    Column("loss_fv_chg", Float, comment="公允价值变动损失"),
    Column("invest_loss", Float, comment="投资损失"),
    Column("decr_def_inc_tax_assets", Float, comment="递延所得税资产减少"),
    Column("incr_def_inc_tax_liab", Float, comment="递延所得税负债增加"),
    Column("decr_inventories", Float, comment="存货的减少"),
    Column("decr_oper_payable", Float, comment="经营性应收项目的减少"),
    Column("incr_oper_payable", Float, comment="经营性应付项目的增加"),
    Column("others", Float, comment="其他"),
    Column("im_net_cashflow_oper_act", Float, comment="经营活动产生的现金流量净额(间接法)"),
    Column("conv_debt_into_cap", Float, comment="债务转为资本"),
    Column("conv_copbonds_due_within_1y", Float, comment="一年内到期的可转换公司债券"),
    Column("fa_fnc_leases", Float, comment="融资租入固定资产"),
    Column("end_bal_cash", Float, comment="现金的期末余额"),
    Column("beg_bal_cash", Float, comment="减:现金的期初余额"),
    Column("end_bal_cash_equ", Float, comment="加:现金等价物的期末余额"),
    Column("beg_bal_cash_equ", Float, comment="减:现金等价物的期初余额"),
    Column("im_n_incr_cash_equ", Float, comment="现金及现金等价物净增加额(间接法)"),
    Column("update_flag", String, comment="更新标识，0未修改1更正过")
]

class TushareCashflow(DailyUpdate, TushareMixin):
    """现金流量表"""

    name = "tushare-cashflow"

    schedule_start_date = (date.today() - timedelta(days=1)).isoformat()
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    columns = cols

    def fetch(self):
        period = self.params['period'] if 'period' in self.params else self.current_period()
        logger.yellow('PERIOD: ' + period)

        data = self.api.cashflow_vip(period=period)
        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data['f_ann_date'] = data['f_ann_date'].fillna(data['ann_date'])
        data = data.rename(columns=dict(f_ann_date = 'trade_date'))
        data = data[[col.name for col in self.columns]]
        data = data.drop_duplicates(["ts_code", "trade_date", "report_type"], keep='last')
        data = data.set_index(["ts_code", "trade_date", "report_type"])
        return data

if __name__ == '__main__':
    updater = TushareCashflow()

    years = list(range(2021, 2022))
    quarters = ['0331', '0630', '0930', '1231']
    data = pd.DataFrame()

    # for year in years:
    #     for quarter in quarters:
            # period = str(year) + quarter
    for period in ['20210930','20211231','20220331','20220630','20220930','20221231',]:
        updater.update(date=date.today().isoformat(), period=period)
        while updater.status == 'error':
            updater.update(date=date.today().isoformat(), period=period)