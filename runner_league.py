import argparse
import collections
import json
import math
import os
import statistics as stats
import sys
import time
import traceback
from multiprocessing import Pool, cpu_count
from models.utils import fmt_tag
import pandas as pd
import tabulate

if os.path.exists(".env"):
    import dotenv
    dotenv.load_dotenv(".env", override=True)
import models.req as req

from models.req import COCRequest
from models.utils import env_istrue

OFFLINE = False

req.OFFLINE = OFFLINE
req.USE_FILES_FOR_REQS = OFFLINE

cocreq = COCRequest()

from models.timer import Timer

jcount = 0

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


def populate_clans():
    clans = {}
    pclans = {}
    res = cocreq.get_locations()
    for e in res["items"]:
        if e["isCountry"]:
            print("populating", e)
            for f in cocreq.get_top_clans_country(e["id"])["items"]:
                clans[f["tag"]] = f["name"]
            for f in cocreq.get_top_players_country(e["id"])["items"]:
                if "clan" in f and "tag" in f["clan"]:
                    if f["clan"]["tag"] not in clans:
                        pclans[f["clan"]["tag"]] = f["clan"]["name"]
    with open("clans.txt", "w") as of:
        for t, n in clans.items():
            of.write(f"{t}\n")
        for t, n in pclans.items():
            of.write(f"{t}\n")
t = Timer()

def finish(auth_token, start, suffix="", league_counts={}):
    from models.model_controler import ModelControler
    from models.models import WarTag
    mc = ModelControler()
    # WarTag._get_unattached_war_tags()

def append_clans(filename, clans):
    with open(filename) as inf:
        for line in inf.readlines():
            vals = line.strip().split(",")
            clans.append(fmt_tag(vals[0]))

def run(auth_token, start, end, suffix="", league_counts={}):
    global jcount
    import time
    st = time.time()
    cpus = cpu_count()
    print("my cpus", cpus)
    import models.db as db
    import models.req as req
    req.OFFLINE = OFFLINE
    db.init_db()
    clans = ["#8ULL0ULU", "#2R9LQRLY", "#8QJY9V8P"]
    # clans = [ "#2R9LQRLY"]
    # clans = ["#8ULL0ULU"]
    # clans = []

    if env_istrue("USE_CLAN_FILE"):
        append_clans("clans_champs3plus_2021_02.csv")
        append_clans("uniq_clans.txt")
    clans = clans[start:] if not end else clans[start:end]

    print("nclans = ", len(clans))
    from models.utils import chunks
    list_clans = chunks(clans, 20)

    cpus = 10
    from models.db import driver

    # if driver.is_sqlite():
    from models.model_controler import ModelControler
    mc = ModelControler()

    for i, c in enumerate(clans):
        try:
            update_league(c, i, start, suffix, league_counts, mc)
        except Exception as e:
            print(e)
            traceback.print_exc()
            raise

    print("done", time.time() - st, req.req_counter.value)
completed = 0

def should_skip(league, counts):
    from models.models import WarLeague
    if counts[league] > 100 and league < WarLeague.master1:
        return True
    if counts[league] > 200 and league <= WarLeague.master1:
        return True
    if counts[league] > 1000 and league <= WarLeague.champ3:
        return True
    return False

def update_league(clans, j, start, suffix="", league_counts={}, mc=None):
    from models.models import WarLeague

    clans = clans if isinstance(clans, list) else [clans]
    found = {}

    global completed

    print("clans", clans)
    if not mc:
        from models.model_controler import ModelControler
        mc = ModelControler()
    for count, ctag in enumerate(clans):
        try:
            print(" - ", start + j, count, end="")
            if ctag in found:
                print()
                continue
            clan = mc.dbcont.get_clan(ctag)
            if clan:
                cl =  clan.get_current_league()
                if cl and should_skip(cl, league_counts):
                    print(f"db skipping {ctag},{cl}, count={league_counts[cl]}")
                    continue

            clan = mc.get_clan(ctag, save_to_db=True)

            if should_skip(clan.war_league, league_counts):
                print(f"skipping {ctag},{clan.war_league}, count={league_counts[clan.war_league]}")
                with open(f"skipped{suffix}.txt", "a") as of:
                    of.write(f"{ctag},{clan.war_league}\n")
                continue
            league_counts[clan.war_league] += 1
            lg = mc.get_league_group(ctag, get_war_log=False, save_to_db=True, get_wars=False)
            print(completed, clan.war_league, "  lg", lg.id if lg else "none", t.ellapsed(), type(clan.war_league))
            if not lg:
                continue
            completed += 1
        except Exception as e:
            print()
            print(e)
            traceback.print_exc()
            continue

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description='Run League.')
    ap.add_argument('--auth-token', default=None)
    ap.add_argument('--env-file', default=None)
    ap.add_argument('--suffix', default="")
    ap.add_argument('--start',type=int, default=0)
    ap.add_argument('--end',type=int, default=None)
    args = ap.parse_args()
    if not args.env_file:
        args.env_file = os.path.join(os.path.dirname(__file__), ".env")


    if os.path.exists(args.env_file):
        import dotenv
        dotenv.load_dotenv(args.env_file, override=True)

    if args.start is None:
        if os.path.exists(f"start{args.suffix}.txt"):
            with open(f"start{args.suffix}.txt") as f:
                for line in f.readlines():
                    args.start = int(line.strip())
                    break

    league_counts = collections.defaultdict(int)
    if os.path.exists(f"counts{args.suffix}.txt"):
        with open(f"counts{args.suffix}.txt") as f:
            for line in f.readlines():
                vals = line.strip().split(',')
                league_counts[int(vals[0].strip())] = int(vals[1])
    print(f"runnning, auth-token={args.auth_token}")
    try:
        try:
            run(args.auth_token, start = args.start, end= args.end, suffix=args.suffix, league_counts=league_counts)
        except KeyboardInterrupt:
            # break
            pass
        except Exception as e:
            print(e)
            raise

    finally:
        with open(f"counts{args.suffix}.txt", "w") as f:
            for k,v in league_counts.items():
                print(k,v)
                f.write(f"{k},{v}\n")
        with open(f"start{args.suffix}.txt", "w") as f:
            f.write(f"{args.start + jcount}\n")

    print("finished")