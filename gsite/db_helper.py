from datetime import date, datetime, timedelta, timezone
from functools import partial

import numpy as np
import pandas as pd
from sqlalchemy.sql import func

from gsite import db
from models.models import PlayerHistory


def add_months(time, months):
    print("month=",time.month)
    month = time.month + months -1
    year = time.year + month // 12
    month = month % 12
    month += 1
    return time.replace(month=month, year=year)

def add_months(time, months):
    print("month=",time.month)
    month = time.month + months -1
    year = time.year + month // 12
    month = month % 12
    month += 1
    return time.replace(month=month, year=year)

def cdtnow():
    return datetime.now(tz=timezone(timedelta(hours=-5), "EST"))


def utc_to_cdt(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=timezone(timedelta(hours=-5), "EST"))


def cdt_to_utc(cdt_dt):
    return cdt_dt.replace(tzinfo=timezone(timedelta(hours=-5), "EST")).astimezone(tz=timezone.utc)


def del_seq_dups(columns, df):
    df.sort_values(columns, inplace=True)
    fcols = df[columns].select_dtypes(include=[np.float]).columns.tolist()
    icols = df[columns].select_dtypes(include=[np.int]).columns.tolist()
    s = df[columns].shift().fillna(-1)
    if fcols:
        s[fcols] = s[fcols].astype(np.float)
    if icols:
        s[icols] = s[icols].astype(int)

    r = df[columns]
    u = (s == r).all(axis=1)
    t = df.loc[~u]
    return t


def get_month(tags, dffuncs=[]):
    tags = tags if isinstance(tags, list) else [tags]
    _now = datetime.now(tz=timezone(timedelta(hours=-5), "EST"))
    st = _now.replace(hour=0, minute=0, second=0, microsecond=0, day=1)
    et = add_months(st, 1)
    et -= timedelta(days=1)

    # st -= timedelta(days=4)

    logs = PlayerHistory.query.filter(
        PlayerHistory.tag.in_(tags), PlayerHistory.last_check > st, PlayerHistory.insert_time < et
    ).order_by(PlayerHistory.insert_time).statement
    df = pd.read_sql(logs, db.session.bind)
    if df.empty:
        return df, st, et

    # df = df.tz_localize('UTC', level=0)
    df["insert_time"] = df["insert_time"].dt.tz_localize("UTC").dt.tz_convert("America/Chicago")
    df["last_check"] = df["last_check"].dt.tz_localize("UTC").dt.tz_convert("America/Chicago")
    # df[['insert_time','last_check']] = df[['insert_time','last_check']].dt.tz_convert('America/Chicago')
    dffuncs = dffuncs if isinstance(dffuncs, list) else [dffuncs]
    for f in dffuncs:
        df = f(df)
    return df, st, et


def get_day(tags, dffuncs=[]):
    tags = tags if isinstance(tags, list) else [tags]
    _now = datetime.now(tz=timezone(timedelta(hours=-5), "EST"))
    st = _now.replace(hour=0, minute=0, second=0, microsecond=0)
    et = _now.replace(hour=23, minute=59, second=0, microsecond=0)
    st = cdt_to_utc(st)
    et = cdt_to_utc(et)
    # st -= timedelta(days=4)
    # print("------------ ", st, et, tags)
    logs = PlayerHistory.query.filter(
        PlayerHistory.tag.in_(tags), PlayerHistory.last_check > st, PlayerHistory.insert_time < et
    ).order_by(PlayerHistory.insert_time).statement
    # print(logs)
    df = pd.read_sql(logs, db.session.bind)

    # df = df.tz_localize('UTC', level=0)
    if df.empty:
        return df, st, et
    df["insert_time"] = df["insert_time"].dt.tz_localize("UTC").dt.tz_convert("America/Chicago")
    df["last_check"] = df["last_check"].dt.tz_localize("UTC").dt.tz_convert("America/Chicago")
    # df[['insert_time','last_check']] = df[['insert_time','last_check']].dt.tz_convert('America/Chicago')
    dffuncs = dffuncs if isinstance(dffuncs, list) else [dffuncs]
    for f in dffuncs:
        df = f(df)
    return df, st, et

