import collections
import json
import math
import os
import statistics as stats
import sys
import time
from itertools import product
from multiprocessing import Pool, cpu_count

import pandas as pd
import tabulate

from models.req import (
    fmt_tag,
    get_clan,
    get_league_group,
    get_league_war,
    get_locations,
    get_top_clans_country,
    get_top_players_country,
    get_war_leagues,
)

OFFLINE = True
USE_REQ = False
DATA_DIR = "data2"
players = {}
league_groups = {}
leagues = {}
check_clans = {
    "2R9LQRLY": "B-Bros",
    "8ULL0ULU": "Official",
    # "8QJY9V8P": "Tribe Gaming",
    # "28CLG222L": ""
}
from models.timer import Timer

df_summary = None
rates_summary = None


def add_stats(df, groupby_field, prefix=""):
    gb = df.groupby([groupby_field])
    counts = gb.size().to_frame(name=f"{prefix}counts")
    ndf = (
        counts.join(gb.agg({"stars": "sum"}).rename(columns={"stars": f"{prefix}total_stars"}))
        .join(gb.agg({"stars": "mean"}).rename(columns={"stars": f"{prefix}stars_mean"}))
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
    # print("btsetn ", max_attacks, missed_attacks)
    df_war_id = get_war_id_df(df)
    # print("@@@@@@@@", df_war_id["team_size"])
    if df_war_id.empty:
        # print(df)
        sys.exit(1)
    max_attacks = df_war_id["team_size"].sum() * 2
    missed_attacks = df_war_id["missed_attacks"].sum()
    # print(max_attacks, missed_attacks, df_war_id.shape)
    tanks = df_war_id["tanked"].sum()
    nwars = df_war_id.shape[0]

    # gb = df.groupby(["attacker_th","defender_th", "stars"])
    gb = df.groupby(["stars"])
    _counts = gb.size().to_frame(name=f"counts").reset_index()
    r = []
    # print("###")
    # print(_counts)
    # print("---------")
    for i in range(4):
        # print(i, i in counts_["stars"], (counts_["stars"] == i).any())
        if not (_counts["stars"] == i).any():
            # counts = counts.append({"stars":i, "count":0}, ignore_index=True)
            r.append([i, 0])
        else:
            b = _counts.loc[_counts['stars'] == i]
            # print("###############################")
            # print("      ", b["counts"].values[0] , " -----------")
            # print("@@@@@@@@@@@@@@@@@@a", _counts)
            r.append([i, b["counts"].values[0] ])
    counts = pd.DataFrame(r, columns=["stars", "counts"])

    # print("************************ counts\n", counts)
    # print("-----------------------------")
    ndf = counts.join(
        gb.agg({"destruction_percentage": "mean"}).rename(
            columns={"destruction_percentage": f"mean_dp"}
        )
    ).reset_index()
    ndf.drop(["index"], inplace=True, axis=1)
    ndf["percentage"] = ndf["counts"] / float(max_attacks)
    # print("###############")
    # print(ndf.to_string())
    # print("###### ----- ", ndf["counts"], "\n @ ", float(max_attacks))
    # print("---------------")
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
    # print(v.to_string())
    # f(counts)

def get_war_id_df(df):
    # print(df)

    adf = df.copy().reset_index()
    # adf['war_id2'] = adf["war_id"]
    # print("mean = ", adf[["war_id", "team_size", "war_id2"]].groupby("war_id", sort=True).transform("mean").shape)
    # print("mean = ", adf[["war_id", "team_size", "war_id2"]].groupby("war_id", sort=True).transform("mean"))
    gb = adf.groupby(["war_id"])
    counts = gb.size().to_frame(name=f"counts").reset_index()
    ndf = counts.reset_index()
    # print("^^^^^^^^^^")
    # print(adf)
    ndf["team_size"] = adf[["war_id", "team_size"]].groupby(["war_id"]).transform("mean")
    ndf["missed_attacks"] = ndf["team_size"] * 2 - ndf["counts"]
    ndf["tanked"] = ndf["missed_attacks"] > ndf["team_size"] * 0.66
    # print(ndf)
    # print("^^^^^^^^^^")
    # sys.exit(1)

    return ndf


def do_league(warleague):
    from models.model_controler import ModelControler
    from models.models import LeagueGroup, LeaguePlayer, LeagueSeason, Player, WarLeague

    mc = ModelControler()

    league = mc.get_league()

    # print("league", league)
    t = Timer()
    # print(" value", warleague)
    df = league.to_attack_df(LeagueGroup.league_id == warleague.value)
    t.ellapsed_print(f"df 1 {df.shape}")
    if df.empty:
        print(f" League {warleague} empty")
        return
    # df = league.to_attack_df2(LeagueGroup.league_id == warleague.value)
    # t.ellapsed_print(f"df 2 {df.shape}")
    # print(df[df.isna().any(axis=1)])
    # print(df.dtypes)
    # print(df)

    # rate_summary(df, rates_summary, "league_summary_rate.csv", warleague)
    au = df["attacker_th"].unique()
    du = df["defender_th"].unique()
    # print(u)

    for ath, dth in product(sorted(au, reverse=True), sorted(du, reverse=True)):
        # print(ath, dth)
        ndf = df[(df["attacker_th"] == ath) & (df["defender_th"]==dth)]
        if not ndf.empty:
            to_summary(ndf, "league_summary_detailed.csv", warleague, ath, dth)
    # print("made it here", os.path.exists("league_summary.csv"))

    to_summary(df, "league_summary.csv", warleague)
    # print("and here?")
    assert os.path.exists("league_summary.csv")
    # print("######## ", os.path.exists("league_summary.csv") )
    if df.shape[0] < 100:
        print("   Not enough attacks")
        return

    df = df.astype({"stars": int})
    t.ellapsed_print(f"toattack_df  {df.shape}")
    if not df.shape[0]:
        return
    adf = add_stats(df, "attacker_tag", "a_")

    adf = adf[adf["attacker_tag"].notna()]
    # adf = adf[adf['a_counts'] > 3]

    ddf = add_stats(df, "defender_tag", "d_")
    # ddf = ddf[ddf["d_counts"] > 3]
    ddf = ddf[ddf["defender_tag"].notna()]

    ndf = adf.merge(ddf, how="outer", left_on="attacker_tag", right_on="defender_tag")
    ndf = ndf[ndf["attacker_tag"].notna()]
    ndf = add_ranks(ndf, "a_")
    ndf = add_ranks(ndf, "d_")

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
    ndf.insert(0, "name", Player.get_names(ndf["attacker_tag"]))
    t.ellapsed_print("insert")
    # a = ["a_count", "a_total_stars", "d_count", "d_total_stars"]
    ndf.to_csv(f"stats_{warleague.value}.csv", index=False)
    # print(ndf)
    print(warleague, "  total = ", t.ellapsed())


def run():
    import time

    t = Timer()

    from models.model_controler import ModelControler
    from models.models import LeagueGroup, LeagueSeason, Player, WarLeague

    mc = ModelControler()
    import models.db as db

    db.init_db()
    # print("league", league)
    # print(league.groups)
    # print(len(league.players))
    # print(len(league._player_name_map))
    t.reset()
    # nmap = league._player_name_map
    t.ellapsed_print("nmap")

    # print([e for e in WarLeague])
    if os.path.exists("league_summary.csv"):
        os.remove("league_summary.csv")
    if os.path.exists("league_summary_detailed.csv"):
        os.remove("league_summary_detailed.csv")

    for warleague in WarLeague:
        # if warleague != WarLeague.champ1:
        #     continue
        do_league(warleague)

    print("total = ", t.totalellapsed())
    # df_summary.to_csv("league_summary.csv", index=False)
    return 1
    # sys.exit(1)
    # s = {k: v for k, v in sorted(players.items(), key=lambda p: p[1].avg_net_stars())}
    s = {
        k: v
        for k, v in sorted(players.items(), key=lambda p: (p[1].attack_stars(), p[1].net_stars()))
    }
    tuples = []
    columns = [
        "name",
        "clan",
        "#Atks",
        "#Defs",
        "Atk ⭐",
        "Def ⭐",
        "Atk ⭐(avg)",
        "Def ⭐(avg)",
        "Net ⭐",
        "Net ⭐ (avg)",
        "Atk% (avg)",
        "Def% (avg)",
    ]
    lsplit = collections.defaultdict(list)
    for __, p in s.items():
        try:
            lsplit[clan_leagues[p.clan]].append(p)
        except:
            continue
    aleg = LeagueStat()
    for lid, plist in sorted(lsplit.items()):
        leg = LeagueStat()
        for p in plist:
            # print(p)
            # print(p.attacks)
            # print(p.defenses)
            for an, a in p.attacks.items():
                try:
                    leg.a[a.key].append(a.stars)
                    leg.ap[a.key].append(a.percent)
                    aleg.a[a.key].append(a.stars)
                    aleg.ap[a.key].append(a.percent)
                except Exception as e:
                    continue
            for dn, d in p.defenses.items():
                try:
                    # print(d)
                    leg.d[d.key].append(d.stars)
                    leg.dp[d.key].append(d.percent)
                except:
                    continue

            if len(p.attacks) < 1:
                continue
            # if p.attack_stars() < 14:
            #     continue
            # if p.clan == "#8ULL0ULU":
            # print(p)
            tuples.append(
                (
                    p.name[:16],
                    clans.get(p.clan, p.clan),
                    len(p.attacks),
                    len(p.defenses),
                    p.attack_stars(),
                    p.defense_stars(),
                    p.avg_attack_stars(),
                    p.avg_defense_stars(),
                    p.net_stars(),
                    p.avg_net_stars(),
                    p.avg_attack_percent(),
                    p.avg_defense_percent(),
                )
            )
        df = pd.DataFrame(tuples, columns=columns)
        df.to_csv(f"out_{lid}.csv", index=False, encoding="utf-8")
        pd.options.display.float_format = "{:,.2f}".format
        print("==== ", lid)
        tups = []
        for k in sorted(leg.a):
            tups.append(
                [
                    int(k.split(":")[0]),
                    int(k.split(":")[1]),
                    len(leg.a[k]),
                    stats.mean(leg.a[k]),
                    stats.mean(leg.ap[k]),
                    leg.percent(k, 0),
                    leg.percent(k, 1),
                    leg.percent(k, 2),
                    leg.percent(k, 3),
                ]
            )
        ldf = pd.DataFrame(
            tups,
            columns=[
                "Attacker TH",
                "Defender TH",
                "# Attacks",
                "Mean Stars",
                "Mean Percent",
                "0 %",
                "1 %",
                "2 %",
                "3 %",
            ],
        )
        ldf.sort_values(by=["Attacker TH", "Defender TH"], inplace=True)
        ldf.to_csv(f"league_{lid}_stats.txt", index=False)
        # print(df.to_string())
        # print(df["Atk ⭐(avg)"].mean(), df["Atk% (avg)"].mean())
        # print(df["Def ⭐(avg)"].mean(), df["Def% (avg)"].mean())
        sdf = df[["name", "clan", "#Atks", "#Defs", "Atk ⭐", "Def ⭐"]]
    tups = []
    for k in sorted(aleg.a):
        tups.append(
            [
                int(k.split(":")[0]),
                int(k.split(":")[1]),
                len(aleg.a[k]),
                stats.mean(aleg.a[k]),
                stats.mean(aleg.ap[k]),
                aleg.percent(k, 0),
                aleg.percent(k, 1),
                aleg.percent(k, 2),
                aleg.percent(k, 3),
            ]
        )
    adf = pd.DataFrame(
        tups,
        columns=[
            "Attacker TH",
            "Defender TH",
            "# Attacks",
            "Mean Stars",
            "Mean Percent",
            "0 %",
            "1 %",
            "2 %",
            "3 %",
        ],
    )
    adf.sort_values(by=["Attacker TH", "Defender TH"], inplace=True)
    adf.to_csv("league_all_stats.txt", index=False)

    # print(tabulate.tabulate(sdf.values, headers=sdf.columns))


def e():
    clans_ = get_top_clans()
    pub = 0
    comp = 0
    for ctag, cname in clans_.items():
        lname = f"data/top_country_clan_{ctag}.json"
        c = get_or_download(lname, get_clan, ctag)

        # c = get_clan(ctag)
        print(cname, c["isWarLogPublic"])
        if c["isWarLogPublic"]:
            pub += 1
        comp += 1
        # if comp > 500:
        #     break
        # break

    print(pub, comp, pub / comp)


if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

    if os.path.exists(dotenv_file):
        import dotenv

        dotenv.load_dotenv(dotenv_file)
    # e()
    # d()
    # populate_clans()
    run()
    # d()
