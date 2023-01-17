import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, Float

from database import logger
from dataset.tushare.__mixin__ import TushareMixin
from engine import DailyUpdate

# ebitda                0.490668 息税折旧摊销前利润
# compr_inc_attr_m_s    0.548040 归属于少数股东的综合收益总额
# compr_inc_attr_p      0.698374 归属于母公司(或股东)的综合收益总额
# t_compr_income        0.698399 综合收益总额
# assets_impair_loss    0.746950 减:资产减值损失
# minority_gain         0.775598 少数股东损益
# invest_income         0.790471 加:投资净收益
# diluted_eps           0.794678 稀释每股收益
# basic_eps             0.818697 基本每股收益
# ebit                  0.848727 息税前利润
# sell_exp              0.932699 减:销售费用
# non_oper_exp          0.941988 减:营业外支出
# income_tax            0.945235 所得税费用
# non_oper_income       0.949239 加:营业外收入
# oper_cost             0.961703 减:营业成本
# fin_exp               0.965011 减:财务费用
# biz_tax_surchg        0.970712 减:营业税金及附加
# admin_exp             0.982414 减:管理费用
# operate_profit        0.993445 营业利润
# total_cogs            0.995849 营业总成本
# n_income              0.996321 净利润(含少数股东损益)
# revenue               0.996362 营业收入
# total_profit          0.997388 利润总额
# total_revenue         0.997393 营业总收入
# n_income_attr_p       0.999106 净利润(不含少数股东损益)

cols = [
    Column("ts_code", String, comment="TS代码", primary_key=True),
    Column("ann_date", Date, comment="公告日期"),
    Column("trade_date", Date, comment="实际公告日期", primary_key=True),
    Column("end_date", Date, comment="报告期"),
    #  1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表 6母公司报表 7母公司单季表 
    # 8母公司调整单季表 9母公司调整表 10母公司调整前报表 11调整前合并报表 12母公司调整前报表
    Column("report_type", String, comment="报告类型", primary_key=True),
    Column("comp_type", String, comment="公司类型(1一般工商业2银行3保险4证券)"),
    # Column("end_type", String, comment="报告期类型"),
    Column("basic_eps", Float, comment="基本每股收益"),
    Column("diluted_eps", Float, comment="稀释每股收益"),
    Column("total_revenue", Float, comment="营业总收入"),
    Column("revenue", Float, comment="营业收入"),
    Column("int_income", Float, comment="利息收入"),
    Column("prem_earned", Float, comment="已赚保费"),
    Column("comm_income", Float, comment="手续费及佣金收入"),
    Column("n_commis_income", Float, comment="手续费及佣金净收入"),
    Column("n_oth_income", Float, comment="其他经营净收益"),
    Column("n_oth_b_income", Float, comment="加:其他业务净收益"),
    Column("prem_income", Float, comment="保险业务收入"),
    Column("out_prem", Float, comment="减:分出保费"),
    Column("une_prem_reser", Float, comment="提取未到期责任准备金"),
    Column("reins_income", Float, comment="其中:分保费收入"),
    Column("n_sec_tb_income", Float, comment="代理买卖证券业务净收入"),
    Column("n_sec_uw_income", Float, comment="证券承销业务净收入"),
    Column("n_asset_mg_income", Float, comment="受托客户资产管理业务净收入"),
    Column("oth_b_income", Float, comment="其他业务收入"),
    Column("fv_value_chg_gain", Float, comment="加:公允价值变动净收益"),
    Column("invest_income", Float, comment="加:投资净收益"),
    Column("ass_invest_income", Float, comment="其中:对联营企业和合营企业的投资收益"),
    Column("forex_gain", Float, comment="加:汇兑净收益"),
    Column("total_cogs", Float, comment="营业总成本"),
    Column("oper_cost", Float, comment="减:营业成本"),
    Column("int_exp", Float, comment="减:利息支出"),
    Column("comm_exp", Float, comment="减:手续费及佣金支出"),
    Column("biz_tax_surchg", Float, comment="减:营业税金及附加"),
    Column("sell_exp", Float, comment="减:销售费用"),
    Column("admin_exp", Float, comment="减:管理费用"),
    Column("fin_exp", Float, comment="减:财务费用"),
    Column("assets_impair_loss", Float, comment="减:资产减值损失"),
    Column("prem_refund", Float, comment="退保金"),
    Column("compens_payout", Float, comment="赔付总支出"),
    Column("reser_insur_liab", Float, comment="提取保险责任准备金"),
    Column("div_payt", Float, comment="保户红利支出"),
    Column("reins_exp", Float, comment="分保费用"),
    Column("oper_exp", Float, comment="营业支出"),
    Column("compens_payout_refu", Float, comment="减:摊回赔付支出"),
    Column("insur_reser_refu", Float, comment="减:摊回保险责任准备金"),
    Column("reins_cost_refund", Float, comment="减:摊回分保费用"),
    Column("other_bus_cost", Float, comment="其他业务成本"),
    Column("operate_profit", Float, comment="营业利润"),
    Column("non_oper_income", Float, comment="加:营业外收入"),
    Column("non_oper_exp", Float, comment="减:营业外支出"),
    Column("nca_disploss", Float, comment="其中:减:非流动资产处置净损失"),
    Column("total_profit", Float, comment="利润总额"),
    Column("income_tax", Float, comment="所得税费用"),
    Column("n_income", Float, comment="净利润(含少数股东损益)"),
    Column("n_income_attr_p", Float, comment="净利润(不含少数股东损益)"),
    Column("minority_gain", Float, comment="少数股东损益"),
    Column("oth_compr_income", Float, comment="其他综合收益"),
    Column("t_compr_income", Float, comment="综合收益总额"),
    Column("compr_inc_attr_p", Float, comment="归属于母公司(或股东)的综合收益总额"),
    Column("compr_inc_attr_m_s", Float, comment="归属于少数股东的综合收益总额"),
    Column("ebit", Float, comment="息税前利润"),
    Column("ebitda", Float, comment="息税折旧摊销前利润"),
    Column("insurance_exp", Float, comment="保险业务支出"),
    Column("undist_profit", Float, comment="年初未分配利润"),
    Column("distable_profit", Float, comment="可分配利润"),
    Column("update_flag", String, comment="更新标识，0未修改1更正过")
]

# 财报数据
class TushareIncome(DailyUpdate, TushareMixin):
    """利润表"""

    name = "tushare-income"

    schedule_start_date = (date.today() - timedelta(days=1)).isoformat()
    schedule_delay = timedelta(days=0)
    schedule_time = time(9, 30)
    schedule_market = "SSE"

    columns = cols

    def fetch(self):
        period = self.params['period'] if 'period' in self.params else self.current_period()
        logger.yellow('PERIOD: ' + period)

        data = self.api.income_vip(period=period)
        data['ann_date'] = data['ann_date'].fillna(data['end_date'])
        data['f_ann_date'] = data['f_ann_date'].fillna(data['ann_date'])
        data = data.rename(columns=dict(f_ann_date = 'trade_date'))
        data = data[[col.name for col in self.columns]]
        data = data.drop_duplicates(["ts_code", "trade_date", "report_type"], keep='last')
        data = data.set_index(["ts_code", "trade_date", "report_type"])
        return data

if __name__ == '__main__':
    updater = TushareIncome()

    years = list(range(2001, 2022))
    quarters = ['0331', '0630', '0930', '1231']
    data = pd.DataFrame()

    # for year in years:
    #     for quarter in quarters:
    #         period = str(year) + quarter
    for period in ['20210930','20211231','20220331','20220630','20220930','20221231',]:
        updater.update(date=date.today().isoformat(), period=period)
        while updater.status == 'error':
            updater.update(date=date.today().isoformat(), period=period)