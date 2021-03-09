import collections
import json
import math
import os
import statistics as stats
import sys
import time
from itertools import product
from multiprocessing import Pool, cpu_count

import elo
import pandas as pd
import tabulate
import trueskill as ts

import models.req as req

_cwd = os.path.dirname(os.path.abspath(__file__))

req.SAVE_DIR="/Users/i855892/data/coc_test2"
_db_path = f"{_cwd}/test_coc2.sqlite"

if os.path.exists(_db_path):
    os.remove(_db_path)

os.environ["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{_db_path}"

import models.db as db

db.init_db()


class Rating():
    def __init__(self):
        self.name = ""
        self.count = 0
        self.wins = 0
        self.trueskill = ts.Rating()
        self.elo = elo.Rating()
        # self.glicko2 = glicko2.Glicko2()

    @property
    def mu(self):
        return self.trueskill.mu

workingdir = os.path.dirname(os.path.abspath(__file__))
def fake(fakeit=True, offline=None):
    import models.req as req
    if offline is None:
        offline = fakeit
    req.SAVE_REQUESTS = fakeit
    req.USE_FILES_FOR_REQS = fakeit
    req.OFFLINE = offline

def run():
    fake(True, False)
    from models.model_controler import ModelControler
    from models.models import fmt_tag
    from models.utils import load_config

    # ratings = collections.defaultdict(ts.Rating)
    cfg = load_config(f"{workingdir}/instance/data/to_check.ini")
    clans = {fmt_tag(k): v for k, v in cfg.items("clans")}
    ratings = {}

    mc = ModelControler()
    import models.models as md

    # war_log = mc.get_war_log(TEST_CLAN, True)
    # war_log = mc.get_war_log(TEST_CLAN, True)
    # print(war_log)
    # war_log = mc.get_war_log("#8ULL0ULU", True)
    # mc.get_war_logs([x.clan2_tag for x in war_log])
    checked_clans = set(list(clans.keys()))
    wl = mc.get_war_logs(list(clans.keys()), save_to_db=True)
    for w in wl or []:
        tags = (x for x in [w.clan1_tag, w.clan2_tag] if x not in checked_clans)
        nts =  []
        for t in tags:
            checked_clans.add(t)
            nts.append(t)
        mc.get_war_logs(nts)
    # for c in clans.keys():
        # print(c)
        # wl = mc.get_war_log(c, save_to_db=True)
        # for w in wl or []:
        #     if not w:
        #         continue
        #     mc.get_war_log(w.clan2_tag, save_to_db=True)
        #     mc.get_war_log(w.clan1_tag, save_to_db=True)

        # print(war.result)
    # sys.exit(1)
    for war in md.War.get_all_wars():
        r1 = ratings.get(war.clan1_tag, Rating())
        r2 = ratings.get(war.clan2_tag, Rating())
        if war.result == 1:
            nr1, nr2 = ts.rate_1vs1(r1.trueskill, r2.trueskill)
            r1.wins +=1
            e1, e2 = elo.rate_1vs1(r1.elo, r2.elo)
            # g1, g2 = glicko2.rate_1vs1(g1.elo, g2.elo)
        else:
            nr2, nr1 = ts.rate_1vs1(r2.trueskill, r1.trueskill)
            r2.wins += 1
            e2, e1 = elo.rate_1vs1(r2.elo, r1.elo)
            # g2, g1 = glicko2.rate_1vs1(g2.elo, g1.elo)

        # print(war.result, war.clan1_tag, f"{r1.mu:.2f} -> {nr1.mu:.2f} ({nr1.mu-r1.mu:.2f})", "   --- ", war.clan2_tag,f"{r2.mu:.2f} -> {nr2.mu:.2f} ({nr2.mu-r2.mu:.2f})")
        r1.trueskill, r2.trueskill = nr1, nr2
        r1.elo, r2.elo = e1, e2
        # r1.glicko2, r2.glicko2 = g1, g2
        r1.count += 1
        r2.count += 1
        ratings[war.clan1_tag] = r1
        ratings[war.clan2_tag] = r2
        # print(war.result)
    for k,v in sorted(ratings.items(),key=lambda x:x[1].trueskill.mu):
        if v.count < 5:
            continue
        print(f"{k}, {clans.get(k, None)} {v.count}, {v.trueskill.mu:.2f}, {v.elo:.0f}, {v.wins/v.count}")

if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

    if os.path.exists(dotenv_file):
        import dotenv

        dotenv.load_dotenv(dotenv_file)
    run()
