import os
from datetime import date, datetime

import pandas as pd
import numpy as np
import toml
from tqdm import tqdm
from sqlalchemy import select, distinct, column

from database import logger
from database.conf import meta, engine

data_path = os.path.join(os.path.dirname(__file__), '../data')
meta_path = os.path.join(os.path.dirname(__file__), '../data/meta')

def load_table(table_name, cond=None):
    table = meta.tables[table_name]
    if cond != None:
        query = select(table).where(cond)
    else:
        query = select(table)
    data = pd.read_sql(query, engine)
    return data

class Backup:
    domain = ''
    data_path = data_path

    def __init__(self, table_name, file_name, cond=None, domain='tushare', date_col='trade_date'):
        self.table_name = table_name
        self.file_name = file_name
        self.domain = domain
        self.date_col = date_col
        self.cond = cond

        self.path = os.path.join(self.data_path, 'backup', self.domain, self.file_name)
        self.meta_path = os.path.join(meta_path, 'backup.toml')

        self.load_meta()

    def meta(self):
        meta_data = dict()

        meta_data['backup_type'] = 'refresh'
        meta_data['length'] = len(self.data)
        meta_data['size'] = int(os.path.getsize(self.path) // 1e6)

        if self.date_col in self.data.columns:
            meta_data['end_date'] = self.data[self.date_col].sort_values().iloc[-1]

        meta_data['data_update_time'] = self.data['_utime'].sort_values().iloc[-1].to_pydatetime()
        # meta_data['update_time'] = datetime.now()
        # meta_data['hash'] = hash(self.data.values.tobytes())

        return meta_data

    def load_meta(self):
        file = toml.load(self.meta_path)

        if self.domain not in file:
            file[self.domain] = dict()

        domain = file[self.domain]
        
        if self.file_name not in domain:
            domain[self.file_name] = dict()

        self.meta_data = domain[self.file_name]

        return file

    def update_meta(self):
        file = self.load_meta()

        meta_data = self.meta()

        for key in meta_data:
            file[self.domain][self.file_name][key] = meta_data[key]

        toml.dump(file, open(self.meta_path, 'w'))

        self.load_meta()

    def load_data(self):
        self.data = load_table(self.table_name, cond = self.cond)

    def dump(self):
        if os.path.exists(self.path):
            os.remove(self.path)

        logger.green('BACKUP: ' + self.table_name)

        self.load_data()

        self.data.to_parquet(self.path, 
                    engine='pyarrow', 
                    compression = 'snappy')

        self.update_meta()

class BackupIncr(Backup):
    def meta(self):
        meta_data = dict()

        meta_data['backup_type'] = 'incr'

        if len(self.data) != 0:
            meta_data['end_date'] = self.data[self.date_col].sort_values().iloc[-1]
            meta_data['data_update_time'] = self.data['_utime'].sort_values().iloc[-1].to_pydatetime()
        # meta_data['update_time'] = datetime.now()

        return meta_data

    def load_data_incr(self):
        if 'data_update_time' in self.meta_data:
            table = meta.tables[self.table_name]

            query = select(table).where(column('trade_date') > self.meta_data['end_date'])

            if self.cond != None:
                query = query.where(self.cond)
            
            self.data = pd.read_sql(query, engine)
        else:
            self.load_data()

    def dump(self, incr_col=None):
        if incr_col is None:
            incr_col = self.date_col

        logger.green('BACKUP: ' + self.table_name)
        
        self.load_data_incr()

        self.data.to_parquet(self.path, 
                        engine='pyarrow', 
                        partition_cols=[incr_col],
                        compression = 'snappy')

        self.update_meta()