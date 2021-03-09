import configparser
import json
import logging
import os
import sys
import timeit
import unittest
from datetime import datetime

import glicko2

_cwd = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, f"{_cwd}/..")

import models.req as req
from models.timer import Timer, timeit

req.SAVE_DIR="/Users/i855892/data/coc_test"
logging.basicConfig(stream=sys.stderr, level=logging.INFO)



_db_path = f"{_cwd}/test_coc.sqlite"


os.environ["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{_db_path}"


from models.model_controler import ModelControler

TEST_CLAN = "#8ULL0ULU"
TEST_CLAN2 = "#2R9LQRLY"
test_clans = {
    # "#2R9LQRLY": "B-BROTHERS",
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
import models.models as md

test_clans = {md.fmt_tag(k): v for k, v in test_clans.items()}

log = logging.getLogger("test_log")

class AllTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        if os.path.exists(_db_path):
            os.remove(_db_path)
        import models.db as db
        db.init_db()

    def tearDown(self):
        import models.db as db
        db.session.close()

    def fake(self, fakeit=True, offline=None):
        import models.req as req
        if offline is None:
            offline = fakeit
        req.SAVE_REQUESTS = fakeit
        req.USE_FILES_FOR_REQS = fakeit
        req.OFFLINE = offline

    @timeit
    def test_war_league(self):
        self.fake(True, True)
        mc = ModelControler()
        lg = mc.get_league_group(TEST_CLAN, save_to_db=True)
        lg2 = mc.get_league_group(TEST_CLAN, save_to_db=True)
        self.assertEqual(lg.id, lg2.id)

    @timeit
    def test_get_clan(self):
        self.fake(True, True)
        mc = ModelControler()
        clan = mc.get_clan(TEST_CLAN, True)
        clan2 = mc.get_clan(TEST_CLAN, True)
        self.assertEqual(clan.tag, clan2.tag)

    @timeit
    def test_get_descriptions(self):
        self.fake(True, True)
        mc = ModelControler()
        clan = mc.get_clan(TEST_CLAN, True)
        cd = clan.description
        cname = clan.name

        clan = mc.get_clan(TEST_CLAN, True)
        self.assertEqual(cd, clan.description)
        self.assertEqual(cname, clan.name)
        r = req.COCRequest()
        d = r.get_clan(TEST_CLAN)
        new_desc =  "blah"
        new_name =  "blah!!"
        d["description"] = new_desc
        d["name"] = new_name
        c = mc._get_player_or_clan(d, md.Clan, md.ClanHistory, md.ClanNameHistory, md.ClanDescriptionHistory, md.EntryType.from_clan, save_to_db=True)

        clan = mc.dbcont.get_clan(TEST_CLAN)
        self.assertEqual(clan.description, new_desc)
        self.assertEqual(clan.name, new_name)

    # @timeit
    # def test_test(self):
    #     self.fake(True, False)
    #     req.DEBUG = True
    #     r = req.COCRequest()
    #     for k,v in test_clans.items():
    #         r.get_clan(k)
    #         try:
    #             r.get_war_log(k)
    #         except Exception:
    #             pass

    def test_league_incomplete(self):
        """ #8ULL0ULU is in a league group with one set of war_tags
        """
        self.fake(True, True)

        mc = ModelControler()
        import models.model_controler as mmc
        from models.models import War
        t = Timer()
        mmc.t.reset()
        lg = mc.get_league_group(TEST_CLAN, save_to_db=True)
        lg2 = mc.get_league_group(TEST_CLAN, save_to_db=True)
        self.assertEqual(lg.id, lg2.id)
        self.assertEqual(lg.state, lg2.state)
        self.assertEqual(8, len(lg.clans))
        for c1, c2 in zip(lg.clans, lg2.clans):
            self.assertEqual(c1.id, c2.id)
        for i, (r1, r2) in enumerate(zip(lg.rounds, lg2.rounds)):
            self.assertEqual(r1.id, r2.id)
            self.assertEqual(r1.war_tags, r2.war_tags)
            self.assertEqual(len(r1.war_tags), 4 if i==0 else 0)
            # print(r1.war_tags)
            for wt in r1.war_tags:
                war = War._get_war_from_war_tag(wt.war_tag)
                # print(war)
                self.assertIsNotNone(war)

    @timeit
    def test_clan_war(self):
        t = Timer()
        self.fake(True, True)
        import models.req as req
        mc = ModelControler()
        war1 = mc.get_current_war(TEST_CLAN, True)
        war2 = md.Clan._get_last_war(TEST_CLAN)
        self.assertEqual(war1.id, war2.id)



    @timeit
    def test_war_log(self):
        self.fake(True, True)
        import collections

        import trueskill as ts

        # ratings = collections.defaultdict(ts.Rating)
        ratings = {}

        mc = ModelControler()
        war_log1 = mc.get_war_log(TEST_CLAN, True)
        war_log2 = mc.get_war_log(TEST_CLAN, True)
        self.assertEqual(len(war_log1), len(war_log2))
        wars = md.War.get_all_wars()
        self.assertEqual(len(war_log1), len(wars))


if __name__ == "__main__":
    # unittest.main()
    # timeit.timeit(AllTests().test_db_load)
    # AllTests().test_war_log()
    AllTests().test_league_incomplete()
