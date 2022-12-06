import pandas as pd
from sqlalchemy import select
from sqlalchemy import distinct

from database.conf import meta

def query(table, cond=None, cols=None):
    if table in meta.tables:
        schema = meta.tables[table]

    if cols is None:
        rp = select(schema)
    else:
        rp = select([schema.c[col] for col in cols])

    if cond is not None:
        rp = rp.where(cond)

    return pd.read_sql(
        rp,
        # index_col=schema.primary_key.columns.keys(),
        con=schema.bind,
    )

def distinct(table, col, cond=None):
    if table in meta.tables:
        schema = meta.tables[table]

    rp = select(schema.c[col]).distinct()

    if cond is not None:
        rp = rp.where(cond)

    return pd.read_sql(
        rp,
        # index_col=schema.primary_key.columns.keys(),
        con=schema.bind,
    )