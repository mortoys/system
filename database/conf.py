from .local import database, tushare_token

import tushare as ts

tushare_api = ts.pro_api(tushare_token)

from sqlalchemy import create_engine

engine = create_engine(database["origin"])

from sqlalchemy import MetaData

meta = MetaData(bind=engine)
meta.reflect()
