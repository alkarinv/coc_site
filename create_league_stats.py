import collections
import json
import math
import os
import statistics as stats
import sys
import time
from datetime import datetime
from itertools import product
from multiprocessing import Pool, cpu_count

import pandas as pd
import tabulate

from models.timer import Timer

OFFLINE = False
OUT_DIR = "stats"
MIN_ATTACKS=50

df_summary = None
rates_summary = None


def add_stats(df, groupby_field, prefix=""):
    gb = df.groupby([groupby_field])
    counts = gb.size().to_frame(name=f"{prefix}counts")
    ndf = (
        counts.join(gb.agg({"stars": "sum"}).rename(columns={"stars": f"{prefix}total_stars"}))
        .join(gb.agg({"stars": "mean"}).rename(columns={"stars": f"{prefix}stars_mean"}))
        .join(gb.agg({"group_id": "mean"}).rename(columns={"group_id": f"{prefix}group"}))
        .join(
            gb.agg({"destruction_percentage": "mean"}).rename(
                columns={"destruction_percentage": f"{prefix}d%_mean"}
            )
        )
        .reset_index()
    )
    a = [f"{prefix}total_stars", f"{prefix}counts"]
    ndf[a] = ndf[a].astype("Int64")
    return ndf


def add_ranks(df, prefix):
    # print(f"  ----------- {prefix} ----------")
    # print(df)
    l = str(df.shape[0])
    df[f"{prefix}rank_stars"] = pd.to_numeric(
        df[f"{prefix}total_stars"].rank(ascending=False, method="min"), downcast="unsigned"
    )
    df[f"{prefix}rank_stars"] = df[f"{prefix}rank_stars"].apply(lambda x: f"{str(x)}/{l}")
    df[f"{prefix}percentile_stars"] = df[f"{prefix}stars_mean"].rank(method="min", pct=True)

    df[f"{prefix}rank_dp"] = pd.to_numeric(
        df[f"{prefix}d%_mean"].rank(ascending=False, method="min"), downcast="unsigned"
    )
    df[f"{prefix}rank_dp"] = df[f"{prefix}rank_dp"].apply(lambda x: f"{str(x)}/{l}")
    df[f"{prefix}percentile_dp"] = df[f"{prefix}d%_mean"].rank(method="min", pct=True)
    return df


def to_summary(df, df_name, warleague, attacker_th = None, defender_th = None):
    df_war_id = get_war_id_df(df)
    if df_war_id.empty:
        sys.exit(1)
    max_attacks = df_war_id["team_size"].sum() * 2
    missed_attacks = df_war_id["missed_attacks"].sum()
    tanks = df_war_id["tanked"].sum()
    nwars = df_war_id.shape[0]

    gb = df.groupby(["stars"])
    _counts = gb.size().to_frame(name=f"counts").reset_index()
    r = []
    for i in range(4):
        if not (_counts["stars"] == i).any():
            r.append([i, 0])
        else:
            b = _counts.loc[_counts['stars'] == i]
            r.append([i, b["counts"].values[0] ])
    counts = pd.DataFrame(r, columns=["stars", "counts"])

    ndf = counts.join(
        gb.agg({"destruction_percentage": "mean"}).rename(
            columns={"destruction_percentage": f"mean_dp"}
        )
    ).reset_index()
    ndf.drop(["index"], inplace=True, axis=1)
    ndf["percentage"] = ndf["counts"] / float(max_attacks)

    a = ["counts", "stars"]
    ndf[a] = ndf[a].astype("Int64")

    v = ndf.unstack().to_frame().sort_index(level=1).T

    def __to_underscore_str__(x):
        return "_".join([str(e) for e in x])

    v.columns = v.columns.map(__to_underscore_str__)
    v.insert(0, "league", warleague.value)
    v["percentage_no_attack"] = missed_attacks / float(max_attacks)
    v["tanked_wars"] = f"{tanks}"
    v["# wars"] = f"{nwars}"
    # print(v[["percentage_no_attack","percentage_0", "percentage_1","percentage_2","percentage_3"]])
    assert math.isclose(
        v["percentage_no_attack"]
        + v["percentage_0"]
        + v["percentage_1"]
        + v["percentage_2"]
        + v["percentage_3"],
        1.0,
    )
    if attacker_th:
        v.insert(1, "defender_th", defender_th)
        v.insert(1, "attacker_th", attacker_th)
    if not os.path.exists(df_name):
        v.to_csv(df_name, index=False)
    else:
        v.to_csv(df_name, index=False, mode="a", header=False)

def get_war_id_df(df):

    adf = df.copy().reset_index()
    gb = adf.groupby(["war_id"])
    counts = gb.size().to_frame(name=f"counts").reset_index()
    ndf = counts.reset_index()
    ndf["team_size"] = adf[["war_id", "team_size"]].groupby(["war_id"]).transform("mean")
    ndf["missed_attacks"] = ndf["team_size"] * 2 - ndf["counts"]
    ndf["tanked"] = ndf["missed_attacks"] > ndf["team_size"] * 0.66

    return ndf


def do_league(warleague, season):
    from models.model_controler import ModelControler
    from models.models import LeagueGroup, LeaguePlayer, LeagueSeason, Player, WarLeague

    mc = ModelControler(None)

    league = mc.get_league(season)
    out_dir = f"{OUT_DIR}/{league.season}"
    os.makedirs(out_dir, exist_ok=True)

    print("league", league, warleague, season, end=" ")
    t = Timer()
    df = league.to_attack_df(LeagueGroup.league_id == warleague.value)
    t.ellapsed_print(f"df {df.shape}")
    if df.empty:
        print(f" League {warleague} empty")
        return

    au = df["attacker_th"].unique()
    du = df["defender_th"].unique()

    for ath, dth in product(sorted(au, reverse=True), sorted(du, reverse=True)):
        ndf = df[(df["attacker_th"] == ath) & (df["defender_th"]==dth)]
        if not ndf.empty:
            to_summary(ndf, f"{out_dir}/league_summary_{league.season}_detailed.csv", warleague, ath, dth)

    to_summary(df, f"{out_dir}/league_summary_{league.season}.csv", warleague)
    assert os.path.exists(f"{out_dir}/league_summary_{league.season}.csv")

    if df.shape[0] < MIN_ATTACKS:
        print("   Not enough attacks")
        return

    df = df.astype({"stars": int})

    if not df.shape[0]:
        return
    adf = add_stats(df, "attacker_tag", "a_")

    adf = adf[adf["attacker_tag"].notna()]

    ddf = add_stats(df, "defender_tag", "d_")
    ddf = ddf[ddf["defender_tag"].notna()]

    ndf = adf.merge(ddf, how="outer", left_on="attacker_tag", right_on="defender_tag")
    ndf = ndf[ndf["attacker_tag"].notna()]
    ndf = add_ranks(ndf, "a_")
    ndf = add_ranks(ndf, "d_")

    ndf = ndf.drop("d_group", axis=1)
    ndf.rename({"a_group":"group"}, inplace=True)

    ndf.sort_values(
        [
            "a_total_stars",
            "a_stars_mean",
            "a_d%_mean",
            "d_total_stars",
            "d_stars_mean",
            "d_d%_mean",
        ],
        ascending=[True, True, True, False, False, False],
        inplace=True,
    )

    ndf.insert(0, "clan_tag", LeaguePlayer.get_clan_tags(ndf["attacker_tag"]))
    ndf.insert(0, "clan", LeaguePlayer.get_clan_names(ndf["attacker_tag"]))
    ndf.insert(0, "name", Player._get_names(ndf["attacker_tag"]))
    ndf.insert(0, 'league_id', value=warleague)
    ndf.insert(0, 'season', value=season)

    ndf.to_csv(f"{out_dir}/league_stats_{league.season}_{warleague.value}.csv", index=False)


def run(season):

    from models.model_controler import ModelControler
    from models.models import LeagueGroup, LeagueSeason, Player, WarLeague
    t = Timer()
    import models.req as req
    req.OFFLINE = OFFLINE

    mc = ModelControler()
    import models.db as db

    db.init_db()
    t.reset()

    for warleague in WarLeague:
        # if not (warleague == WarLeague.champ3 or warleague == WarLeague.master1):
        #     continue
        do_league(warleague, season)
        t.ellapsed_print("warleague", warleague)

    print("total time = ", t.totalellapsed())
    return 1


if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(__file__), ".env")

    if os.path.exists(dotenv_file):
        import dotenv
        dotenv.load_dotenv(dotenv_file)
    season = datetime.today().replace(day=1).date()
    print("season = ", season)

    run(season)
