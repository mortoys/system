import os
import pandas as pd
from datetime import date, time, datetime, timedelta

from database import tushare_api as api
from database.track import track
from database import logger

path = os.path.join(os.path.dirname(__file__), '..', 'data/date/')

def sync():
    name = 'tushare-trade_cal'
    if track[name].date() == date.today():
        return

    logger.green("FETCHING: CAL SSE")
    data = api.trade_cal(is_open=1, exchange='SSE')
    data.to_pickle(path + 'SSE.pkl')

    data = api.trade_cal(exchange='SSE')
    data.to_pickle(path + 'ALL.pkl')

    logger.green("FETCHING: CAL XHKG")
    dd1 = api.trade_cal(is_open=1, exchange='XHKG')
    dd2 = api.hk_tradecal(is_open=1)
    data = pd.concat([dd1[['cal_date']], dd2[['cal_date']]]).drop_duplicates().sort_values('cal_date')
    data.to_pickle(path + 'XHKG.pkl')

    logger.green("FETCHING: CAL US")
    data = api.us_tradecal(is_open=1)
    data.to_pickle(path + 'US.pkl')

    track.update(name, date.today())

def get_date(start_date, end_date, exchange = 'SSE', localize=False):
    df = pd.read_pickle(path + exchange + '.pkl')
    df['cal_date'] = pd.to_datetime(df['cal_date'])

    start_date = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else start_date
    end_date = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else end_date

    df = df[(df['cal_date'] >= start_date) & (df['cal_date'] <= end_date)]
    date = df['cal_date']
    date = date.rename("trade_date")

    if localize:
        date = date.dt.tz_localize('Asia/Shanghai')

    return date

# def need_update(last_dt=None, table_name=None, 
#     schedule_time=time(0), schedule_delay=timedelta(0), schedule_market='SSE', 
#     cur=datetime.now()):

#     if last_dt == None:
#         last_dt = track[table_name]

#     start_date = last_dt.date() + timedelta(days=1)

#     # 当前时间在获取时间之后 则算今天
#     if cur.time() >= schedule_time:
#         end_date = cur.date() - schedule_delay
#     else:
#         end_date = cur.date() - schedule_delay + timedelta(days=-1)

#     # print(start_date, end_date)
#     valid_days = load(start_date, end_date, schedule_market)

#     return valid_days