import base64
import calendar
import io
from datetime import date, datetime, timedelta, timezone
from functools import partial

import click
import matplotlib
import matplotlib.style as style
import numpy as np
from flask.cli import with_appcontext

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # ## fixes a bug that crashes mac mojave
import seaborn as sns

# sns.set_style("darkgrid")
style.use("fivethirtyeight")
import matplotlib.dates as mdates
import pandas as pd
from sqlalchemy.sql import func

import gsite.db_helper as dbh
from models import db
from models.models import PlayerHistory

__days_fmt = mdates.DateFormatter("%d")
__hours_fmt = mdates.DateFormatter("%H")


def get_img_data(clear=True):
    img = io.BytesIO()
    plt.savefig(img, format="png")
    img.seek(0)
    d = base64.b64encode(img.getvalue()).decode()
    if clear:
        ## Clear plot for new plots
        plt.clf()  #  clear data and axes
    return d


def __label_first_last(df):
    df.sort_values(["tag", "insert_time"], inplace=True)
    df = df.reset_index()
    df["oindex"] = df.index
    first = df.groupby("tag").first().reset_index()
    print(first.to_string())
    # df["tolabel"] = df.apply(lambda x: x.index in first.oindex, axis=1)
    df["tolabel"] = df.index.isin(first["oindex"])
    last = df.groupby("tag").last()
    last["insert_time"] = dbh.cdtnow()
    last["tolabel"] = True
    # last = last.reset_index()
    return df.append(last, sort=False)


def label_point(x, y, val, ax, color, tolabel):
    a = pd.concat({"x": x, "y": y, "val": val, "color": color, "tolabel": tolabel}, axis=1)
    for i, point in a.iterrows():
        if not point["tolabel"]:
            continue
        ax.text(point["x"], point["y"], str(point["val"]), color=point["color"])


def __label_colors(df):
    unique = df["name"].unique()
    palette = dict(zip(unique, sns.color_palette()))
    df["colors"] = df.name.apply(lambda x: palette[x])
    return df, palette


def graph_player_day(tags, show=False):
    df, st, et = dbh.get_day(tags, partial(dbh.del_seq_dups, ["tag", "trophies"]))
    if df.empty:
        #### don't understand how matplotlib handles this and can't make it work
        return
    df = __label_first_last(df)
    df, palette = __label_colors(df)
    # df.insert_time = df.insert_time.dt.tz_convert('CDT')
    if not df.empty:
        df.insert_time = df.apply(lambda x: x.insert_time.tz_convert("US/Central"), axis=1)
        df["insert_time"] = df.insert_time.dt.tz_localize(None)
    else:
        df['insert_time'] = pd.to_datetime(df.insert_time.astype(str))
    st -= timedelta(hours=5)
    et -= timedelta(hours=5)

    # fig,ax = plt.subplots()

    ax = sns.lineplot(
        data=df,
        x="insert_time",
        y="trophies",
        # marker="o",
        hue="name",
        drawstyle="steps-post",
        palette=palette,
    )

    ax.set_xlim(st - timedelta(hours=1), et + timedelta(hours=1))
    ax.xaxis_date(tz="US/Central")
    ax.set_xlabel("Hour of Day")
    ax.set_ylim(4800, None)
    ops = {"color": "gray", "linestyle": "--", "linewidth": 2}
    ymin, ymax = ax.get_ylim()
    ax.vlines([st, et], ymin=ymin, ymax=ymax - 1, **ops)

    ax.xaxis.set_major_locator(mdates.HourLocator())
    ax.xaxis.set_major_formatter(__hours_fmt)

    label_point(df.insert_time, df.trophies, df.trophies, plt.gca(), df.colors, df.tolabel)

    if show:
        plt.show()

    # fig.autofmt_xdate()


def graph_player_history(tags, show=False):
    tags = tags if isinstance(tags, list) else [tags]
    # _now = cdtnow()
    # st = _now.replace(hour=0, minute=0, second=0, microsecond=0, day=1)
    # et = st + timedelta(weeks=4)
    # et -= timedelta(day=1)
    # st = cdt_to_utc(st) - timedelta(hours=5)
    # et = cdt_to_utc(et) - timedelta(hours=5)
    df, st, et = dbh.get_month(tags, partial(dbh.del_seq_dups, ["tag", "trophies"]))
    if df.empty:
        return
    df = __label_first_last(df)
    df, palette = __label_colors(df)

    df.insert_time = df.apply(lambda x: x.insert_time.tz_convert("US/Central"), axis=1)
    df["insert_time"] = df.insert_time.dt.tz_localize(None)

    # logs = (
    #     PlayerHistory.query.filter(
    #         PlayerHistory.tag.in_(tags), PlayerHistory.last_check > st, PlayerHistory.insert_time < et
    #     )
    #     .order_by(PlayerHistory.last_check)
    #     .statement
    # )
    # df = pd.read_sql(logs, db.session.bind)
    # df, palette = __label_colors(df)
    # print(df[['name', 'insert_time','trophies', 'tolabel']].to_string())
    fig, ax = plt.subplots()

    ax = sns.lineplot(
        data=df,
        x="insert_time",
        y="trophies",
        # marker="o",
        hue="name",
        drawstyle="steps-post",
        palette=palette,
    )
    ax.set_xlim(st - timedelta(hours=5, days=1), et - timedelta(hours=4) + timedelta(days=1))
    ax.xaxis_date(tz="US/Central")
    ax.set_xlabel("Days of Month")
    ax.set_ylim(4800, None)
    ops = {"color": "gray", "linestyle": "--", "linewidth": 2}
    ymin, ymax = ax.get_ylim()
    ax.vlines([st, et], ymin=ymin, ymax=ymax - 1, **ops)
    # ax.axvline(et, **ops)

    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(__days_fmt)
    # ax.xaxis.set_minor_locator(mdates.HourLocator())

    label_point(df.insert_time, df.trophies, df.trophies, plt.gca(), df.colors, df.tolabel)

    if show:
        plt.show()
    fig.autofmt_xdate()


@click.command("plot")
@with_appcontext
def plot_player():
    matplotlib.use("TkAgg")
    tags = ["#JPCVVPYY", "#JLRGG0RL", "#YRCCG8U", "#JVRYY9UJ", "#LLCCLV2L"]
    graph_player_day(tags, show=True)
    # graph_player_day(["#JPCVVPYY", "#JLRGG0RL"], show=True)
    # graph_player_day(get_day(["#JPCVVPYY"]))
    # graph_player_day(["#JLRGG0RL"], show=True)

    graph_player_history(tags, show=True)
    click.echo("Plotted.")


def init_app(app):
    app.cli.add_command(plot_player)
