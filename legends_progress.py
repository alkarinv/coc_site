import configparser
import enum
import os
import random
import sys
from datetime import datetime

import models.req as req
from models.utils import chunks

workingdir = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(dotenv_file):
        import dotenv

        print(f"Loading dotenv '{dotenv_file}'")
        dotenv.load_dotenv(dotenv_file)

from models.model_controler import League, ModelControler
from models.models import fmt_tag

fake = True
if fake:
    import models.db as db

    req.use_last = True
    db.fake = True

MIN_WAIT_BETWEEN_REQUESTS = 40
MAX_WAIT_BETWEEN_REQUESTS = 7200 - 60
RAND_WAIT_MAX = 240
RAND_WAIT_MIN = 60
__force = "--force" in sys.argv

args = {"fake": False, "force": __force}

mc = ModelControler()

__dynamic_config_file = f"{workingdir}/instance/data/dynamic.ini"
print(__dynamic_config_file, flush=True)
__config = configparser.RawConfigParser()
__config.read(f"{workingdir}/instance/data/to_check.ini")
always_check_players = {fmt_tag(k): v for k, v in __config.items("players")}
always_check_clans = {fmt_tag(k): v for k, v in __config.items("clans")}


def __read_or_create_dynamic_ini(cp=None, cc=None):
    config = configparser.RawConfigParser()
    if not os.path.exists(__dynamic_config_file):
        config.add_section("players")
        config.add_section("clans")
        with open(__dynamic_config_file, "w") as configfile:
            config.write(configfile)
    config.read(__dynamic_config_file)
    if cp:
        cp.update({fmt_tag(k): v for k, v in config.items("players")})
    if cc:
        cc.update({fmt_tag(k): v for k, v in config.items("clans")})
    return config


__read_or_create_dynamic_ini(always_check_players, always_check_clans)



def check_all():
    print("Checking", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
    p = mc.get_player("#JPCVVPYY")
    # print(p)
    # print(p.clan_tag, p.clan_name, p.trophies, p.donations)
    # return
    found = set()
    for __clan in always_check_clans:
        print(" Clan", __clan, always_check_clans[__clan])
        ptc = mc.get_clan_members(__clan, True)
        found |= ptc.keys()

    missing = set(always_check_players.keys()) - found
    # print(ptc.keys())
    print("Found ", len(always_check_clans), len(found))
    print("missing=", len(missing), [v for k, v in always_check_players.items() if k in missing])
    config = __read_or_create_dynamic_ini()
    for t in missing:
        p = mc.get_player(t)
        print(p, p.clan_tag, p.last_history())
        if p and p.tag and p.clan_tag:
            config.set("clans", p.clan_tag.replace("#", "").upper(), p.clan_name.replace("%", "%%"))

    # print(always_check_players)


if __name__ == "__main__":
    # import cProfile
    # pr = cProfile.Profile()
    # pr.enable()
    # # cProfile.run('check_players(True)')
    check_all()
    # pr.disable()x
    # pr.print_stats(sort='time')
