from datetime import datetime
from sqlalchemy import Column, Integer, String, Table, DateTime, JSON

from database.conf import meta

schema = Table("log", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("typ", String, index=True, comment="事件类型"),
    Column("name", String, comment="事件名称"),
    Column("params", JSON, default={}, comment="事件参数"),
    Column("desc", String, comment="事件描述"),
    Column("_utime", DateTime, default=datetime.now, comment="事件发生时间"),
    comment="事件",
    extend_existing=True,
)
schema.create(checkfirst=True)


def add(name, desc="", params={}, typ="info"):
    p = params.copy()
    for key, val in p.items():
        if hasattr(val, "isoformat"):
            p[key] = val.isoformat()[:19]

    schema.insert().values(name=name, typ=typ, desc=desc, params=p).execute()


def info(name, desc="", params={}):
    add(name, desc, params, "info")


def error(name, desc="", params={}):
    add(name, desc, params, "error")


class bcolors:
    ORANGE = "\033[94m"
    BLUE = "\033[96m"
    GREEN = "\033[92m"
    PURPLE = "\033[95m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    NORMAL = "\033[0m"
    ENDC = "\033[0m"

from time import time
t = time()
def ex(msg, color="NORMAL"):
    global t
    s = "{:>3.0f}".format(time() - t)
    t = time()
    print(f"[{s}] {getattr(bcolors, color)}{msg}{bcolors.ENDC}")


def orange(msg):
    ex(msg, "ORANGE")


def blue(msg):
    ex(msg, "BLUE")


def green(msg):
    ex(msg, "GREEN")


def purple(msg):
    ex(msg, "PURPLE")


def yellow(msg):
    ex(msg, "YELLOW")


def red(msg):
    ex(msg, "RED")


if __name__ == "__main__":
    add("aaa", {"a": 1})
