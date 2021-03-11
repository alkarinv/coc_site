import argparse
import collections
import json
import math
import os
import statistics as stats
import sys
import time
import traceback
from datetime import datetime
from multiprocessing import Pool, cpu_count

import pandas as pd
import tabulate

import models.req as req
from models.req import COCRequest

OFFLINE = False

req.OFFLINE = OFFLINE
req.USE_FILES_FOR_REQS = OFFLINE

cocreq = COCRequest()

from models.timer import Timer

jcount = 0


t = Timer()


def finish(auth_token, start, suffix="", league_counts={}):
    from models.model_controler import ModelControler
    from models.models import WarTag

    mc = ModelControler()
    # WarTag._get_unattached_war_tags()


def run(
    auth_token,
    start=0,
    end=None,
    suffix="",
    season=None,
    get_wars=False,
    get_war_logs=False,
    league_counts={},
):
    global jcount
    completed = 0
    import time

    st = time.time()
    cpus = cpu_count()
    print(f"Finishing start={start}, end={end}, get_wars={get_wars}")
    import models.db as db
    import models.req as req

    req.OFFLINE = OFFLINE
    db.init_db()
    from models.model_controler import ModelControler

    mc = ModelControler()

    from models.models import LeagueSeason, WarLeague
    if not season:
        season = datetime.today().replace(day=1).date()

    season = LeagueSeason.get_season(season)
    groups = season.groups[start:] if not end else season.groups[start:end]
    print(f"season={season}, ngroups={len(groups)}")
    # print(groups[0], season.season, type(season.season))
    # sys.exit(1)
    # print("clan", clans)
    for count, group in enumerate(groups):
        try:
            print(" - ", start + count, end=" ")
            if group.finished():
                print(" ", group, count, "#")
                continue
            mc.get_league_group(
                group.clans[0].tag, season=season.season, get_wars=get_wars, save_to_db=True
            )
            # for c in group.clans:
            #     mc.get_war_log(c.tag, save_to_db=True)
            # found.update({c.tag:1 for c in lg.clans})
            completed += 1
            print(" ", count, completed, " %")
        except Exception as e:
            print()
            print(e)
            traceback.print_exc()
            continue
            # raise
            ## add clan to errored out list
    print("done", time.time() - st, req.req_counter.value)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run League.")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--env-file", default=None)
    ap.add_argument("--suffix", default="")
    ap.add_argument("--get-wars", action="store_true")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=None)
    args = ap.parse_args()
    if not args.env_file:
        args.env_file = os.path.join(os.path.dirname(__file__), ".env")

    if os.path.exists(args.env_file):
        import dotenv

        dotenv.load_dotenv(args.env_file, override=True)
    print(f"runnning, auth-token={args.auth_token}")
    try:
        run(
            args.auth_token,
            start=args.start,
            end=args.end,
            suffix=args.suffix,
            get_wars=args.get_wars,
        )
        # finish(args.auth_token, start = args.start, suffix=args.suffix, league_counts=league_counts)
    except KeyboardInterrupt:
        # break
        pass
    except Exception as e:
        print(e)
        raise

    print("finished")