import streamlit as st
import numpy as np
from database import query
from database import column, and_, or_
from datetime import date
from plotnine import ggplot, geom_point, aes, stat_smooth, facet_wrap

from visual.industry import main as industry_indicator
from visual.holdernumber import main as holdernumber
from visual.hk_hold import main as hk_hold
from visual.margin_detail import main as margin_detail
from visual.asset import main as asset
from visual.income import main as income

page = st.sidebar.selectbox('页面', ['资产', '利润表', '行业估值', '筹码建模-股东数', '筹码建模-北向资金', '筹码建模-融资融券'])

if page == '资产':
    asset()

if page == '利润表':
    income()

if page == '行业估值':
    industry_indicator()

if page == '筹码建模-股东数':
    holdernumber()

if page == '筹码建模-北向资金':
    hk_hold()

if page == '筹码建模-融资融券':
    margin_detail()