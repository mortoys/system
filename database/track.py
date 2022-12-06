from datetime import datetime
from sqlalchemy import Table, Column, String, DateTime, JSON

from database.conf import meta

schema = Table(
    "track",
    meta,
    Column("table", String, primary_key=True, comment=""),
    Column("last_dt", DateTime, comment='最新的数据时间'),
    Column("params", JSON, comment='最后一次更新的参数'),
    Column("comment", String),
    Column("_utime", DateTime, default=datetime.now),
    comment="跟踪数据最后更新时间",
    extend_existing=True,
)
schema.create(checkfirst=True)

class Track:
    def __init__(self):
        rows = schema.select().execute().fetchall()

        self.dict = {}
        for row in rows:
            self.dict[row['table']] = row['last_dt']

    def __getitem__(self, key):
        assert key in self.dict
        return self.dict[key]

    def __contains__(self, key):
        return key in self.dict

    def update(self, table, dt, params={}, comment=''):
        for key in params:
            if hasattr(params[key], 'isoformat'):
                params[key] = params[key].isoformat()

        if table not in self.dict.keys():
            rp = schema.insert().values(table=table, 
                last_dt=dt, params=params, comment=comment, _utime=datetime.now())
        else:
            rp = (schema
                .update()
                .where(schema.c.table == table)
                .values(last_dt=dt, params=params, comment=comment, _utime=datetime.now()))

        rp.execute()
        self.dict[table] = dt

#     def __iter__(self):
#         return self.dict.__iter__()

#     def __repr__(self):
#         return schema.select().execute().fetchall()

#     def __str__(self):
#         return self.__repr__().__str__()

#     def __len__(self):
#         return len(self.dict)

track = Track()
