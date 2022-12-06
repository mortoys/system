from os.path import join

import pandas as pd
import numpy as np

from plydata import call, select, rename
from plydata.tidy import gather
from plotnine import ggplot, scale_color_brewer, ylim, scale_x_datetime, aes, facet_wrap
from plotnine import ggtitle, stat_smooth, geom_line, geom_col, geom_blank

from .process import ic, ir, ret_neutral, ret_mean
from .io import assemble

# import matplotlib
# sorted([f.name for f in matplotlib.font_manager.fontManager.ttflist])
from plotnine import themes, element_text
themes.theme_set(themes.theme_minimal() + themes.theme(text=element_text(family="Source Han Sans CN")))


def save_fig(title, plot, path='/Users/lumotian/Downloads/'):
    (plot + ggtitle(title)).save(filename = join(path, title + '.png'), height=5, width=15, units = 'in')
    return plot + ggtitle(title)

ret_mean_date = call(lambda df: df.mean().to_frame().T.melt())

ret_cumsum = call(lambda df: df.cumsum().melt(ignore_index=False).reset_index())

factor_turnover = call(lambda df: 
    df['qrank'].unstack().diff().abs().mean(axis=1)
       .rolling(30).mean().to_frame('fee').reset_index())

def plot_fee(title):
    return call(lambda df: save_fig(title,
        df >> factor_turnover
        >> ggplot()
        + aes(x='date', y='fee')
        + geom_line()
        + scale_x_datetime(date_labels='%Y', date_breaks='1 year')))

from functools import reduce

ic_data = call(lambda factor: reduce(
    lambda x, y: pd.merge(x, y,
        on = 'date',
        how = 'inner'
    ), [
    factor >> ic('ret_1'),
    factor >> ic('ret_5'),
    factor >> ic('ret_10'),
    factor >> ic('ret_22'),
    factor >> ic('ret_62')])
    >> gather('period', 'ic', select('-date'))
)

ir_data = call(lambda factor: reduce(
    lambda x, y: pd.merge(x, y,
        on = 'date',
        how = 'inner'
    ), [
    factor >> ir('ret_1'),
    factor >> ir('ret_5'),
    factor >> ir('ret_10'),
    factor >> ir('ret_22'),
    factor >> ir('ret_62')])
    >> gather('period', 'ir', select('-date'))
)

def plot_ic(title):
    return call(lambda df: save_fig(title,
        df >> ic_data
        >> ggplot()
            + aes('date', 'ic', color='period')
            + geom_line() 
            + stat_smooth(geom='smooth', method='lowess', span=0.33)
            + scale_x_datetime(date_labels='%Y', date_breaks='1 year')
            + ylim(-0.2, 0.2)))

def plot_ir(title):
    return call(lambda df: save_fig(title,
        df >> ir_data
        >> ggplot()
            + aes('date', 'ir', color='period')
            + geom_line() 
            + stat_smooth(geom='smooth', method='lowess', span=0.33)
            + scale_x_datetime(date_labels='%Y', date_breaks='1 year')
            + ylim(-2, 2)))

def plot_bar(title, base=False):
    return call(lambda df: save_fig(title,
        df >> ret_mean_date
        >> ggplot()
            + aes('group', 'value')
            + geom_col(position='dodge')
            + (facet_wrap('base') if base else geom_blank())))

def plot_cum(title, base=False):
    return call(lambda df: save_fig(title,
        df >> ret_cumsum
        >> ggplot()
            + aes('date', 'value', color='group')
            + geom_line()
            + scale_x_datetime(date_labels='%Y', date_breaks='1 year')
            + scale_color_brewer(type='div', palette='RdYlBu', direction=-1)
            + (facet_wrap('base') if base else geom_blank())))


def plot(topic, name, env_name, env):
    factor = assemble(topic, name, env)

    ret = factor >> ret_mean(group = 's5_2') >> ret_neutral
    ret >> plot_bar('bar_' + name + '_' + env_name)
    ret >> plot_cum('cum_' + name + '_' + env_name)

    ret = factor >> ret_mean(group = 's5_2', base='_s3', func=np.median) >> ret_neutral
    ret >> plot_bar('bar_' + name + '_size_' + env_name, base=True)
    ret >> plot_cum('cum_' + name + '_size_' + env_name, base=True)
    
    factor >> plot_fee('fee_' + name + '_' + env_name)

    factor >> plot_ic('IC_' + name + '_' + env_name)
    factor >> plot_ir('IR_' + name + '_' + env_name)

# (corr.melt(ignore_index=False).reset_index()
# >> ggplot()
#     + aes('index', 'variable', fill='value')
#     + geom_tile()
#     + scale_fill_distiller(type='div', palette=5)
# )