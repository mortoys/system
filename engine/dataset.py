from datetime import datetime
import pandas as pd
import time
from sqlalchemy import Table, Column, DateTime

from database import track, logger, meta
from .register import Register


class Dataset(metaclass=Register):

    def __init__(self):
        self.create_table()

        self.status = 'init'

        self.params = {}

    def create_table(self):
        if self.name in meta.tables:
            self.table = meta.tables[self.name]
        else:
            self.table = Table(
                self.name,
                meta,
                *self.columns,
                Column("_utime", DateTime, default=datetime.now),
                comment=self.__doc__,
            )
            self.table.create(checkfirst=True)

    def fetch(self) -> pd.DataFrame:
        pass

    def log(self):
        logger.info(self.name, "success", self.params)

        # track.update(self.name, datetime.now(), self.params, self.__doc__)

    def insertDb(self):
        self.dataframe.to_sql(
            self.name,
            con=meta.bind,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )


    def update(self, **kargs):
        self.params = { **self.params, **kargs }

        logger.green("START: " + self.name + "  " + str(self.params))

        self.dataframe = None

        while(type(self.dataframe) is not pd.DataFrame or self.status == 'error'):
            try:
                logger.green("FETCHING: " + str(self.params))
                self.dataframe = self.fetch()

                if len(self.dataframe) == 0:
                    "数据为空"
                    logger.red("数据为空")
                    logger.error(self.name, "数据为空", self.params)
                    self.status = 'empty'
                else:
                    self.dataframe["_utime"] = datetime.now()
                    self.status = 'success'
            except Exception as e:
                logger.red(str(e)[:1000])
                logger.error(self.name, str(e)[:1000], self.params)
                self.status = 'error'

                if hasattr(e.args[0], 'startswith') and e.args[0].startswith('抱歉，您每分钟最多访问该接口'):
                    logger.yellow('Frequency limitation')
                    time.sleep(20)

        if self.status == 'success':

            logger.green(f"RECORDING: length {len(self.dataframe)}")

            self.insertDb()

            logger.info(self.name, "success", self.params)

            logger.green("SUCCESS: " + self.name)

        return self.dataframe

    def query(self, cond=None):
        rp = self.table.select()

        if cond is not None:
            rp = rp.where(cond)

        return pd.read_sql(
            rp.with_only_columns(self.columns),
            # index_col = self.table.primary_key.columns.keys(),
            con = self.table.bind,
        )
