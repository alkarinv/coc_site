import collections
import json
import math
import os
import statistics as stats
import sys
import time

import pandas as pd
import tabulate

from models.req import (
    fmt_tag,
    get_clan,
    get_league_group,
    get_league_war,
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
clans = {}
clan_leagues = {}
error_clans = {}
if os.path.exists("error_clans.txt"):
    with open("error_clans.txt") as f:
        for l in f.readlines():
            error_clans[l.strip()] = 1


class LeagueStat:
    def __init__(self):
        self.a = collections.defaultdict(list)
        self.d = collections.defaultdict(list)
        self.ap = collections.defaultdict(list)
        self.dp = collections.defaultdict(list)

    def percent(self, k, stars):
        return self.a[k].count(stars) / len(self.a[k]) if self.a else math.nan

    def star_percent(self, k, stars):
        return self.ap[k].count(stars) / len(self.ap[k]) if self.ap else math.nan


class Clan:
    def __init__(self, json_data):
        self.tag = json_data["tag"]
        self.name = json_data["name"]


class Attack:
    def __init__(self, json_data):
        self.stars = json_data["stars"]
        self.percent = json_data["destructionPercentage"]
        self.attacker_tag = json_data["attackerTag"]
        self.defender_tag = json_data["defenderTag"]
        self.order = json_data["order"]

    @property
    def key(self):
        # return players[self.attacker_tag].townHallLevel*100 + players[self.defender_tag].townHallLevel
        return (
            f"{players[self.attacker_tag].townHallLevel}:{players[self.defender_tag].townHallLevel}"
        )

    def __str__(self):
        tha = players.get(self.attacker_tag, "").townHallLevel
        thd = players.get(self.defender_tag, "").townHallLevel
        return f"    {self.attacker_tag} ({tha}) -> {self.defender_tag} ({thd}) : {self.stars} ({self.percent})"


class LeaguePlayer:
    def __init__(self, clan, json_data):
        self.clan = clan
        self.tag = json_data["tag"]
        self.name = json_data["name"]
        self.townHallLevel = json_data["townHallLevel"]
        self.attacks = collections.OrderedDict()
        self.defenses = collections.OrderedDict()

    def add_attack(self, r, json_data):
        self.attacks[r] = Attack(json_data[0])

    def add_defense(self, r, json_data):
        self.defenses[r] = Attack(json_data)

    def net_stars(self):
        return self.attack_stars() - self.defense_stars()

    def attack_stars(self):
        return sum([e.stars for e in self.attacks.values()])

    def defense_stars(self):
        return sum([e.stars for e in self.defenses.values()])

    def attack_percent(self):
        return sum([e.percent for e in self.attacks.values()])

    def defense_percent(self):
        return sum([e.percent for e in self.defenses.values()])

    def avg_attack_stars(self):
        return self.attack_stars() / len(self.attacks) if self.attacks else math.nan

    def avg_defense_stars(self):
        return self.defense_stars() / len(self.defenses) if self.defenses else math.nan

    def avg_net_stars(self):
        try:
            return self.avg_attack_stars() - self.avg_defense_stars()
        except:
            return math.nan

    def avg_attack_percent(self):
        try:
            return self.attack_percent() / len(self.attacks)
        except:
            return math.nan

    def avg_defense_percent(self):
        try:
            return self.defense_percent() / len(self.defenses)
        except:
            return math.nan

    def __str__(self):
        # return f"{self.name}: {self.clan}, {len(self.attacks)}:{len(self.defenses)}"
        return (
            f"{self.name}, {self.clan}, {len(self.attacks)}:{len(self.defenses)} (#atks:#defs),"
            f" {self.attack_stars()}:{self.defense_stars()} (total stars atk:def)  "
            f"{self.avg_attack_stars():.2f}:{self.avg_defense_stars():.2f} (average stars atk:def)  "
            f" net={self.net_stars()}  avgnet={self.avg_net_stars():.2f}"
        )


def get_or_download(fname, func, *args, force=False):
    if os.path.exists(fname) and not force:
        with open(fname) as f:
            return json.load(f)
    elif not OFFLINE:
        data = func(*args)
        os.makedirs(os.path.dirname(fname), exist_ok=True)
        with open(fname, "w") as of:
            json.dump(data, of)
        return data
    else:
        return None

def run_get_locs():
    fname = f"data/locations.json"
    with open(fname) as f:
        data = json.load(f)
    for loc in data["items"]:
        # print(loc)
        if loc["isCountry"]:
            lname = f"{DATA_DIR}/top_country_{loc['id']}.json"
            top_list = get_or_download(lname, get_top_players_country, loc["id"])
            for i, pl in enumerate(top_list["items"]):
                # if len(check_clans) >= 100:
                #     break
                if "clan" in pl:
                    check_clans[pl["clan"]["tag"]] = pl["clan"]["name"]


    # for tag, name in check_clans.items():
    #     # print(name)
    #     t = tag.replace("#","")
    #     fname = f"data/league/leaguegroup_tag_{t}.json"


def get_top_clans():
    fname = f"{DATA_DIR}/locations.json"
    with open(fname) as f:
        data = json.load(f)
    clans_ = {}
    for loc in data["items"]:
        # print(loc)
        if loc["isCountry"]:
            lname = f"data/top_country_clans_{loc['id']}.json"
            # print(lname)
            top_list = get_or_download(lname, get_top_clans_country, loc["id"])
            for i, pl in enumerate(top_list["items"]):
                # if len(check_clans) >= 100:
                #     break
                clans_[pl["tag"]] = pl["name"]
    return clans_

def find_league(clan_tags):
    global clan_leagues
    ls = 0
    for tag in clan_tags:
        cname = f"{DATA_DIR}/clans/{tag}.json"
        cdata = get_or_download(cname, get_clan, tag)
        ls += cdata["warLeague"]["id"]
    l = round(ls / len(clan_tags))
    for tag in clan_tags:
        clan_leagues[fmt_tag(tag)] = l
    # print("League ", l, clan_tags)
    return l


def run_league():
    found = {}
    completed = 0
    for count, (c, cname) in enumerate(check_clans.items()):
        c = fmt_tag(c)
        if c in error_clans:
            continue
        if c in clan_leagues:
            # print("Already found", c)
            continue

        try:
            fname = f"{DATA_DIR}/league/leaguegroup_{c}.json"
            if not os.path.exists(fname):
                fname = f"{DATA_DIR}/league/leaguegroup_{c.replace('#', '')}.json"
            leaguedata = get_or_download(fname, get_league_group, c)
            if not leaguedata:
                continue
        except:
            with open("error_clans.txt", "a") as of:
                of.write(c + "\n")
            continue
        clan_tags = []
        for i, r in enumerate(leaguedata["clans"]):
            clans[r["tag"]] = r["name"]
            clan_tags.append(r["tag"])
            for m in r["members"]:
                tag = m["tag"]
                if tag not in players:
                    player = LeaguePlayer(r["tag"], m)
                    players[tag] = player

        try:
            league = find_league(clan_tags)
        except:
            continue
        # print(count, completed, league, clan_tags)
        # if league != 48000011:
        #     continue
        completed += 1
        # if completed > 50:
        #     break

        for i, r in enumerate(leaguedata["rounds"]):
            # print(f"round {i}")
            # print(r["warTags"])
            warTags = r["warTags"]
            for wt in warTags:
                # print("    ", wt)
                if wt in found:
                    print("Already found", wt)
                    continue
                found[wt] = True
                fname = f'{DATA_DIR}/league/warTag_{wt.replace("#","")}.json'
                data = get_or_download(fname, get_league_war, wt)
                if data and data["state"] == "inWar":
                    data = get_or_download(fname, get_league_war, wt, force=True)
                if not data:
                    continue
                for e in ["clan", "opponent"]:
                    for m in data[e]["members"]:
                        # print(m)
                        clan_tag = data[e]["tag"]
                        tag = m["tag"]
                        if tag not in players:
                            player = LeaguePlayer(clan_tag, m)
                            players[tag] = player
                        else:
                            player = players[tag]
                        # print("#", m)
                        if "attacks" in m:
                            player.add_attack(i, m["attacks"])
                        if "bestOpponentAttack" in m:
                            player.add_defense(i, m["bestOpponentAttack"])
                    # print(player)


def fill_leagues():
    global leagues
    data = get_or_download("{DATA_DIR}/warleague.json", get_war_leagues)
    for l in data["items"]:
        leagues[l["id"]] = l["name"]

#

def d():
    try:
        run_get_locs()
        run_league()
    except Exception as e:
        print(e)
        raise
    fill_leagues()
    print(leagues)
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
        comp +=1
        # if comp > 500:
        #     break
        # break


    print(pub, comp, pub/comp)

if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

    if os.path.exists(dotenv_file):
        import dotenv

        dotenv.load_dotenv(dotenv_file)
    # e()
    d()