import base64
import calendar
import collections
import io
from datetime import date, datetime, timedelta, timezone
from functools import partial

import click
import numpy as np
import pandas as pd
from flask.cli import with_appcontext
from sqlalchemy.sql import func

import gsite.db_helper as dbh
from gsite import db
from models.models import PlayerHistory

__star_hash = {}

for __i in range(0, 41):
    if __i <= 4:
        __star_hash[__i] = 0
    elif __i <= 15:
        __star_hash[__i] = 1
    elif __i <= 32:
        __star_hash[__i] = 2
    elif __i <= 40:
        __star_hash[__i] = 3

print(__star_hash)


@click.command("table")
@with_appcontext
def get_day():
    tags = ["#JPCVVPYY", "#JLRGG0RL", "#YRCCG8U", "#JVRYY9UJ", "#LLCCLV2L"]
    # df, __, __ = dbh.get_day(tags, partial(dbh.del_seq_dups, ["tag", "trophies"]))
    df = pd.read_csv("table.csv")
    df.sort_values("insert_time", inplace=True)

    players = collections.defaultdict(dict)

    for i, row in df.iterrows():
        # print(i, type(row), row)
        tag = row["tag"]
        p = players[tag]
        t = row["trophies"]
        p["trophies"] = t
        p["name"] = row["name"]
        pt = p.get("prev_trophies", t)
        a = p.get("a", 0)
        d = p.get("d", 0)
        if t > pt:
            p["a"] = a + 1
        elif t < pt:
            p["d"] = d + 1
        # print(tag, t, pt)
        p["prev_trophies"] = t

    pdf = pd.DataFrame(players.values(), index=tags, columns=["name", "trophies", "a", "d"]).fillna(
        0
    )

    for c in ["a", "d"]:
        pdf[c] = pdf[c].astype(np.int)
    # print(pdf)
    return pdf


def resolve(pdict, trophies, typ):
    """currently doesn't work

    Arguments:
        pdict {[type]} -- [description]
        trophies {[type]} -- [description]
        typ {[type]} -- [description]
    """
    pass
    # array = p[typ]
    # l = np.full((1, trophies // 40 + 1), trophies / (trophies // 40 + 1)).flatten()
    # print(p['tag'], "   resolving", l, array, trophies)
    # array.extend(l)
    # stars = [__star_hash[int(e)] for e in l]
    # for s in stars:
    #     p["%s_%i"%(typ, s)] += 1
    # print("   stars= ", stars)

@click.command("get_rate")
@with_appcontext
def get_rate(tags):
    tags = ["#JPCVVPYY"]
    return get_rates(tags)

def get_rates(tags):
    tags = [x.upper() for x in tags]
    # df, __, __ = dbh.get_history(tags, partial(dbh.del_seq_dups, ["tag", "trophies"]))
    # print("mY tags = ", tags)
    df, __, __ = dbh.get_month(tags)
    # print("my df = ", df)
    return __get_rate(df)

def __get_rate(df):
    # df = pd.read_csv("table.csv")
    df.tag = df.tag.str.upper()
    df.sort_values(["tag", "insert_time"], inplace=True)

    players = collections.defaultdict(dict)
    for t in set(df.tag.tolist()):
        t = t.upper()
        # print("#@@@@@@@@@@@@@@@", t)
        players[t]["a"] = []
        players[t]["d"] = []
        players[t]["a_t"] = 0
        players[t]["d_t"] = 0
        players[t]["tag"] = t
        for i in range(0,4):
            players[t]["a_%d"%i] = 0
            players[t]["d_%d"%i] = 0
    # print("#######################", len(players))
    pt = None
    oldtag = None
    for i, row in df.iterrows():
        tag = row["tag"]
        if tag != oldtag:
            pt = None
            oldtag = tag
        print("    ", i, row.tag, row.trophies, row.insert_time, row.last_check)
        # if tag not in tags: ### tags??
        #     continue
        p = players[tag]
        t = row["trophies"]
        if not pt or t == pt:
            ### this might be a "drop" attack where they purposefully lost, but more likely it
            ### is a second entry due to an update not being performed over enough time
            pt = t
            # print(pt, t)
            continue
        p["trophies"] = t
        p["name"] = row["name"]

        # pt = p.get("prev_trophies", t)
        typ = None
        if t > pt:
            change = t - pt
            typ = "a"
        elif t < pt:
            change = pt - t
            typ = "d"
        # print(typ, t - pt)
        if typ:
            if change > 40:
                resolve(p, change, typ)
            else:
                p[typ].append(change)
                p["%s_t"%typ] += change
                stars = __star_hash[change]
                p["%s_%i"%(typ, stars)] += 1
        p["prev_trophies"] = t
        pt = t
        oldtag = tag
        # print("!!!!!", type(p), p)
    # print("@@@@@@@", players, type(players["#JPCVVPYY"]))
    for k, p in players.items():
        # print("#ENESNTENST", type(p), p)
        for i in range(0,4):
            p["pa_%d"%i] = int(p["a_%d"%i] / len(p["a"])*100) if len(p["a"]) else 0
            p["pd_%d"%i] = int(p["d_%d"%i] / len(p["d"])*100) if len(p["d"]) else 0


    # pdf = pd.DataFrame(
    #     players.values(), index=tags, columns=["name", "trophies", "a", "d", ""]
    # ).fillna(0)

    # for c in ["a", "d"]:
    #     pdf[c] = pdf[c].astype(np.int)

    # for k, v in players.items():
    #     print(k, v)

    # print(pdf)
    # print(players)
    return players


def init_app(app):
    """Register database functions with the Flask app. This is called by
    the application factory.
    """
    app.cli.add_command(get_rate)


