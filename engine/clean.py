import os
from os.path import join

import pandas as pd
import toml

import resource

resource.setrlimit(resource.RLIMIT_NOFILE, (10000, -1))

from database import logger

data_path = join(os.path.dirname(__file__), '../data')
meta_path = join(os.path.dirname(__file__), '../data/meta')

class Clean:
    freq = ''
    asset = ''

    def __init__(self):
        self.name = self.asset + '-' + self.freq

        self.backup_path = join(data_path, 'backup')
        self.file_path = join(data_path, 'clean', self.name)
        self.meta_path = os.path.join(meta_path, 'clean', self.name + '.toml')

    def get_data(self, columns):
        return pd.read_parquet(self.file_path, columns=columns)

    def meta(self):
        meta_data = dict()

        if 'meta' not in meta_data:
            meta_data['meta'] = dict()
            meta = meta_data['meta']
            meta['length'] = self.data.shape[0]
            meta['columns'] = self.data.shape[1]
            meta['duplicate'] = self.data.index.to_frame(index=None).duplicated().sum()

        for col in self.data.columns:
            if col not in meta_data:
                meta_data[col] = dict()
                meta_col = meta_data[col]
                meta_col['nan_ratio'] = self.data[col].isna().mean()

        return meta_data

    def load_meta(self):
        if os.path.exists(self.meta_path):
            file = toml.load(self.meta_path)
        else:
            file = dict()

        self.meta_data = file

        return file

    def update_meta(self):
        file = self.load_meta()

        if not hasattr(self, 'data'):
            self.data = pd.read_parquet(self.file_path)

        meta_data = self.meta()

        for column in meta_data:
            if column not in file:
                file[column] = dict()
            for key in meta_data[column]:
                file[column][key] = meta_data[column][key]

        toml.dump(file, open(self.meta_path, 'w'))

        self.load_meta()

    def load(self, file):
        logger.green('LOAD: ' + file)
        return pd.read_parquet(join(self.backup_path, file)).drop('_utime', axis=1)
    
    def loads(self, files):
        for file in files:
            yield self.load(file)

    def dump(self):
        logger.green('CLEAN: ' + self.name)

        self.process()

        if hasattr(self, 'complete'):
            logger.green('COMPLETE: ' + self.name)
            self.complete()

        logger.green('DUMP: ' + str(self.data.shape))

        self.data.to_parquet(
            join(data_path, 'clean', self.name),
            engine='pyarrow', 
            compression='gzip',
            index=True)

        self.update_meta()