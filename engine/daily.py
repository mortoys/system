from datetime import datetime, timedelta, time

from database import get_date, track, logger, meta
from .dataset import Dataset


class DailyDataset(Dataset):
    freq = timedelta(days=1)

    schedule_delay = timedelta(days=0)
    schedule_time = time(0, 0)
    schedule_market = "SSE"

    def update(self, date, **kargs):
        self.date = date

        dataframe = super().update(date=date, **kargs)

        if self.status == 'success':
            track.update(self.name, date, self.params, self.__doc__)

        return dataframe

    @classmethod
    def needUpdateDays(self, last_dt=datetime.now(), cur=datetime.now()):

        # 不包括 last_dt
        start_date = last_dt.date() + timedelta(days=1)

        # 当前时间在获取时间之后 则算今天
        if cur.time() >= self.schedule_time:
            end_date = cur.date() - self.schedule_delay
        else:
            end_date = cur.date() - self.schedule_delay + timedelta(days=-1)

        valid_days = get_date(start_date, end_date, self.schedule_market)

        # import pandas_market_calendars as mcal

        return [{"date": date} for date in valid_days]

class DailyRefresh(DailyDataset):
    schedule_time = time(0, 0)

    def update(self, **kargs):
        with self.table.bind.connect() as con:
            con.execute(self.table.delete())

        return super().update(**kargs)

    @classmethod
    def needUpdateDays(self, last_dt=datetime.now(), cur=datetime.now()):
        
        if last_dt.date() != cur.date() and cur.time() >= self.schedule_time:
            return [{"date": cur}]
        else:
            return []


from pangres import upsert

# DailyUpdate 数据要求返回的是 set_index 之后的
class DailyUpdate(DailyDataset):
    schedule_time = time(9, 30)

    def insertDb(self):
        upsert(
            engine=meta.bind,
            df=self.dataframe,
            table_name=self.name,
            if_row_exists='update',
        )

    @classmethod
    def needUpdateDays(self, last_dt=datetime.now(), cur=datetime.now()):
        
        if last_dt.date() != cur.date() and cur.time() >= self.schedule_time:
            return [{"date": cur}]
        else:
            return []