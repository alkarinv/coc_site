import configparser
import json
import logging
import os
import sys
import timeit
import unittest

from models.utils import add_method

test_clans = {
    "#2R9LQRLY": "B-BROTHERS",
    "#8ULL0ULU": ".:OffiCiaL:.",
    "#8QJY9V8P": "Tribe Gaming",
    "#9GP02C22": "WHF",
    "#28L9CVQVV": "AsianClanTop1",
    "#PGRL2U0Y": "AsianClanTop2",
    "#9RGVL2QQ": "Semper Invicta",
    "#8YPUUV0L": "Cream Esports",
    "#RQ9YULUL": "orksGP",
    "#2J990RU0": "JapaneseClan1",
    "#G992JJ0Q": "kweearkar",
    "#98YRQYYL": "hansupdontchoot",
    "#PUYRPP9Q": "syria one",
}

def get_insert_db_path():
    _cwd = os.path.dirname(os.path.abspath(__file__))

    _db_path = f"{_cwd}/test_coc.sqlite"
    os.environ["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{_db_path}"
    print(os.environ["SQLALCHEMY_DATABASE_URI"])
    return _db_path

def init():
    _db_path = get_insert_db_path()
    if os.path.exists(_db_path):
        os.remove(_db_path)

    import models.db as db
    db.init_db()


log = logging.getLogger("test_log")

def fake(fakeit=True):
    import models.db as db
    import models.req as req
    req.USE_FILES_FOR_REQS = fakeit
    req.SAVE_REQUESTS = fakeit
    db.fake = fakeit

def test_db_load1():
    from models.model_controler import ModelControler
    mc = ModelControler()
    # print("len, len", mc.table_len_player(),mc.table_len_player_history())
    mc.get_clan_members(list(test_clans.keys()), save_to_db=True)

def test_db_load2():

    def get_clan_members2(self, tags, save_to_db=False):
        from datetime import datetime

        import models.db as db
        import models.req as req
        from models.models import Player, PlayerHistory, __fmt_tag__
        tags = tags if isinstance(tags, list) else [tags]
        players = {}

        for tag in tags:
            tag = __fmt_tag__(tag)
            clandict = req.get_clan_members(tag)

            hists = {}
            ## We need to save players and histories separately
            ## because there isn't an insert or replace
            for pdict in clandict["items"]:
                pdict["entry_type"] = EntryType.FROM_CLAN_MEMBERS

                p, __ = db.get_or_create(
                    db.session, Player, create_kwargs=pdict, filter_kwargs={"tag": pdict["tag"]}
                )
                ph = PlayerHistory(**pdict)
                hists[p.tag] = ph
                if save_to_db:
                    lh = self.get_last_history(p.tag)
                    if lh != ph:
                        db.session.add(ph)
                    else:
                        lh.last_check = datetime.utcnow()
                        lh.update()  ## update the check time

                players[p.tag] = p

                # print("lh=", self.get_last_history(p.tag))
        if save_to_db:
            db.session.commit()
        return players

    from models.model_controler import ModelControler
    mc = ModelControler()
    add_method(mc, get_clan_members2)
    mc.get_clan_members2(list(test_clans.keys()), save_to_db=True)


def test_db_load3(clans=None):
    def get_clan_members3(self, tag, save_to_db=False):
        from datetime import datetime

        import models.db as db
        import models.req as req
        from models.models import Player, PlayerHistory, __fmt_tag__

        players = {}
        tag = __fmt_tag__(tag)
        clandict = req.get_clan_members(tag)

        hists = {}
        ## We need to save players and histories separately
        ## because there isn't an insert or replace
        for pdict in clandict["items"]:
            pdict["entry_type"] = EntryType.FROM_CLAN_MEMBERS

            p, __ = db.get_or_create(
                db.session, Player, create_kwargs=pdict, filter_kwargs={"tag": pdict["tag"]}
            )
            ph = PlayerHistory(**pdict)
            hists[p.tag] = ph
            if save_to_db:
                lh = self.get_last_history(p.tag)
                if not lh or lh != ph:
                    db.session.add(ph)
                else:
                    lh.last_check = datetime.utcnow()
                    lh.update()  ## update the check time

            players[p.tag] = p

            # print("lh=", self.get_last_history(p.tag))
        if save_to_db:
            db.session.commit()
        return players

    from models.model_controler import ModelControler
    mc = ModelControler()
    _test_clans = clans if clans else test_clans
    add_method(mc, get_clan_members3)
    for c in _test_clans:
        mc.get_clan_members3(c, save_to_db=True)




if __name__ == "__main__":
    # unittest.main()
    timeit.timeit(test_db_load1)
