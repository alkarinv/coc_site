import collections
import json
import math
import os
import statistics as stats
import sys
import time
import traceback
from multiprocessing import Pool, cpu_count

import pandas as pd
import tabulate
from sqlalchemy import and_, or_

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
from models.timer import Timer

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

t = Timer()


def run():
    import time
    st = time.time()
    import models.db as db
    import models.req as req
    from models.model_controler import ModelControler
    from models.models import War, WarAttack, WarClan, WarPlayer
    mc = ModelControler()
    lg = mc.get_league()
    ws = []
    # wars = War.query.all()
    t.ellapsed_print("queried war")
    # for g in lg.groups:
    #     for r in g.rounds:
    #         for w in r.wars:
    #             print("w ", w)
    #             ws.append(w)
    # db.session.commit()
    # a = [w.id for w in wars]
    from models.utils import chunks
    with db.engine.begin() as connection:
        q = """SELECT war_player_id, id FROM war_attack WHERE war_attack.war_id IS NULL AND war_player_id is NOT NULL """
        ras = connection.execute(q)
        # print("len " list(r))
        for wal in chunks(list(ras), 1000):
            update_vals = []
            dwa = {e[0]:e[1] for e in wal if e[0] is not None}
            war_player_ids = ",".join([str(x) for x in list(dwa.keys())])
            q = f"""SELECT war_attack.war_player_id, war.id  FROM war_attack JOIN war_player ON war_player.id = war_attack.war_player_id JOIN war_clan ON war_clan.id = war_player.war_clan_id JOIN war ON war_clan.war_id = war.id WHERE war_player.id in ({war_player_ids})  """
            r = connection.execute(q)
            for e in list(r):
                v = {"id":dwa[e[0]], "war_id":e[1]}
                update_vals.append(v)

            # break

            db.session.bulk_update_mappings(WarAttack, update_vals)
            db.session.commit()
            t.ellapsed_print("doing 1k")
    t.ellapsed_print("finished")
    print("done", time.time() - st)

# def run():
#     import time
#     st = time.time()
#     import models.db as db
#     import models.req as req
#     from models.models import War, WarAttack, WarPlayer, WarClan
#     from models.model_controler import ModelControler
#     mc = ModelControler()
#     lg = mc.get_league()
#     ws = []
#     # wars = War.query.all()
#     t.ellapsed_print("queried war")
#     # for g in lg.groups:
#     #     for r in g.rounds:
#     #         for w in r.wars:
#     #             print("w ", w)
#     #             ws.append(w)
#     # db.session.commit()
#     # a = [w.id for w in wars]
#     from models.utils import chunks
#     with db.engine.begin() as connection:
#         q = """SELECT war_player_id, id FROM war_attack WHERE war_attack.war_id IS NULL AND war_player_id is NOT NULL limit 10000 """
#         ras = connection.execute(q)
#         # print("len " list(r))
#         update_vals = []
#         for wal in chunks(list(ras), 1000):
#             dwa = {e[0]:e[1] for e in wal if e[0] is not None}
#             war_player_ids = ",".join([str(x) for x in list(dwa.keys())])
#             q = f"""SELECT war_attack.war_player_id, war.id  FROM war_attack JOIN war_player ON war_player.id = war_attack.war_player_id JOIN war_clan ON war_clan.id = war_player.war_clan_id JOIN war ON war_clan.war_id = war.id WHERE war_player.id in ({war_player_ids})  """
#             r = connection.execute(q)
#             for e in list(r):
#                 v = {"id":dwa[e[0]], "war_id":e[1]}
#                 update_vals.append(v)
#             break
#         s.bulk_update_mappings(MyTable, update_vals)
#         # dwa = {e[0]:e[1] for e in list(r)}

#         # dwc = """SELECT war_clan_id

#         # for i, wid in enumerate(a):
#         #     if i % 100 == 0:
#         #         t.ellapsed_print(f"{i} {wid}")
#         #     q = """SELECT war_attack.war_id AS war_attack_war_id FROM war_attack JOIN war_player ON war_player.id = war_attack.war_player_id JOIN war_clan ON war_clan.id = war_player.war_clan_id  WHERE war_clan.war_id = :war_id AND war_attack.war_id IS NULL """
#         #     r = connection.execute(q, {"war_id":wid})
#         #     l = len(list(r))
#         #     # print("len == ", l)
#         #     if l == 0:
#         #         continue
#         #     q = f"""SELECT war_attack.id AS war_attack_id FROM war_attack JOIN war_player ON war_player.id = war_attack.war_player_id JOIN war_clan ON war_clan.id = war_player.war_clan_id WHERE war_clan.war_id=:war_id"""

#         #     r = connection.execute(q, {"war_id":wid})
#         #     res = [x[0] for x in r]
#         #     # print(len(res))
#         #     # print(res)
#         #     d = {f":vals{i}":res[i] for i in range(len(res))}
#         #     vals = " , ".join([str(x) for x in list(d.values())])
#         #     # d.update({'war_id': wid})
#         #     # print(d)
#         #     q = f"UPDATE war_attack SET war_id=:war_id WHERE war_attack.id IN ({vals})"
#         #     print(q)
#         #     # continue
#         #     r = connection.execute(q, {'war_id': wid})
#         #     # print(r)

#     t.ellapsed_print("blh")
#     print("done", time.time() - st)
completed = 0

if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

    if os.path.exists(dotenv_file):
        import dotenv
        dotenv.load_dotenv(dotenv_file)
    run()