import configparser
import enum
import os
import random
import sys
from datetime import datetime
from functools import lru_cache

from sqlalchemy import and_, func, not_, or_, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError, MultipleResultsFound, NoResultFound
from sqlalchemy.sql import exists, func

import models.db as db
from models.exceptions import NotInWarException, WarLogPrivateException
from models.models import (
    Clan,
    ClanDescriptionHistory,
    ClanHistory,
    ClanNameHistory,
    EntryType,
    LeagueClan,
    LeagueGroup,
    LeagueSeason,
    Player,
    PlayerHistory,
    PlayerNameHistory,
    War,
    WarState,
    fmt_tag,
)
from models.req import COCRequest
from models.timer import Timer, timeit
from models.utils import chunks

found = 0

CLAN_CHUNK_SIZE = 20  ## number of clans to update at once
MIN_TIME_TO_UPDATE = 10  # in seconds

sentinel = object()

t = Timer()


class DBControler:
    def __init__(self):
        db.init_db()
        # pass

    def get_season(self, season):
        f = (
            func.DATETIME(LeagueSeason.season) == func.DATETIME(season)
            if db.driver.is_sqlite()
            else LeagueSeason.season == season
        )
        return LeagueSeason.query.filter(f).one_or_none()

    def get_unfinished_wars(self):
        f = (
            func.DATETIME(War.end_time) >= func.DATETIME(datetime.utcnow())
            if db.driver.is_sqlite()
            else War.end_time <= datetime.utcnow()
        )
        return War.query.filter(f).all()

    def table_len_player_history(self):
        return db.session.query(PlayerHistory.id).count()

    def table_len_player(self):
        return db.session.query(PlayerHistory.id).count()

    def players_exist(self, tags):
        fs = db.session.query(Player.tag).filter(Player.tag.in_(tags)).all()
        return set([x[0] for x in fs])

    def players_not_exist(self, tags):
        fs = db.session.query(Player.tag).filter(Player.tag.in_(tags)).all()
        return set(tags) - set([x[0] for x in fs])

    def player_exists(self, tag):
        return db.session.query(Player.tag).filter_by(tag=tag).scalar() is not None

    def clan_exists(self, tag):
        return db.session.query(Clan.tag).filter_by(tag=tag).scalar() is not None

    def get_player(self, tag):
        return Player.query.filter(Player.tag == tag).one_or_none()

    def get_clan(self, tag):
        return Clan.query.filter(Clan.tag == tag).one_or_none()

    def get_last_history(self, tag):
        if isinstance(tag, (Player, PlayerHistory)):
            tag = tag.tag
        return (
            PlayerHistory.query.filter(PlayerHistory.tag == tag)
            .order_by(PlayerHistory.last_check.desc())
            .first()
        )

    def _get_name_changed(self, obs, ocls):
        if not obs:
            return None
        conditions = (and_(ocls.tag == p["tag"], ocls.name != p["name"]) for p in obs)
        q = ocls.query.filter(or_(*conditions))
        return q.all()

    def _get_description_changed(self, obs):
        if not obs:
            return None
        # conditions = (and_(Clan.tag==p["tag"], Clan.name!=p["name"]) for p in obs)
        # conditions =
        # q = Clan.query.filter(or_(*conditions))

        stmt = f"""WITH ranked_clan_history AS
                    (SELECT m.*, ROW_NUMBER()
                    OVER (PARTITION BY last_check ORDER BY tag DESC ) AS rn
                    FROM clan_description_history AS m WHERE TAG IN ({tags}))
                    SELECT id, tag, description
                    FROM ranked_clan_history
                    WHERE rn = 1"""
        res = db.execute(stmt)
        # print(res)
        return q.all()

    def _insert_players_or_clans(self, obsdict, ocls, name_cls, desc_cls=None):
        """Does not update history
        """
        # print("_insert_players_or_clans", list(obsdict.keys()))
        new_ps = ocls._get_not_exist(list(obsdict.keys()))
        insert_ps = {}
        found_ps = {}
        for k, v in obsdict.items():
            if k in new_ps:
                insert_ps[k] = v
            else:
                found_ps[k] = v

        changed = ocls._get_name_changed(found_ps.values())
        dchanged = ocls._get_descriptions_changed(found_ps.values()) if desc_cls else None
        if insert_ps:
            db.session.execute(ocls.__table__.insert(), list(insert_ps.values()))
        for p in changed or []:
            # print("playersdict", playersdict, p.tag, p.name)
            p.name = found_ps[p.tag]["name"]
            db.session.merge(p)
            pnh = name_cls(**{"tag": p.tag, "name": p.name})
            db.session.add(pnh)
            p.names.append(pnh)
        for p in dchanged or []:
            # print("playersdict", playersdict, p.tag, p.name)
            pnh = desc_cls(**{"tag": p.tag, "description": found_ps[p.tag]["description"]})
            db.session.add(pnh)

        # print("####### end insert_clans_or_players")

    def get_war(self, war):
        if isinstance(war, War):
            # return self.get_war_from_war(war)
            return War.get_war(war.clan1_tag, war.clan2_tag, war.end_time)
        else:
            return self.get_war_from_war_tag(war)

    def get_war_from_war_tag(self, war_tag):
        try:
            return db.session.query(War).filter(War.war_tag == war_tag).one()
        except NoResultFound:
            return None

    def has_war_from_war_tag(self, war_tag):
        try:
            return db.session.query(War.id).filter(War.war_tag == war_tag).one()
        except NoResultFound:
            return None

    def in_league(self, clan_tag):
        try:
            return (
                db.session.query(LeagueClan.in_league).filter(LeagueClan.tag == clan_tag).scalar()
            )
        except MultipleResultsFound:
            r = db.session.query(LeagueClan.in_league).filter(LeagueClan.tag == clan_tag).all()
            return r[0] == True
        except NoResultFound:
            return None

    def set_in_league(self, clan_tag, season_id, in_league):
        lc = LeagueClan(season_id, **{"tag": clan_tag, "in_league": in_league})
        db.session.add(lc)
        db.session.commit()

    def get_league_group(self, clan_tag, season_id):
        print(clan_tag, season_id)
        lgid = (
            db.session.query(LeagueClan.league_group_id)
            .filter(and_(LeagueClan.league_season_id == season_id, LeagueClan.tag == clan_tag))
            .one_or_none()
        )

        return LeagueGroup.query.filter(LeagueGroup.id == lgid[0]).one_or_none() if lgid else None
        return res


class ModelControler:
    def __init__(self, coc_request=sentinel):
        self.dbcont = DBControler()
        self.req = COCRequest() if coc_request == sentinel else coc_request

    def get_league(self, season=None):
        if not season:
            season = self.get_current_season()
        return self.dbcont.get_season(season)

    def get_clan_members(self, tags, save_to_db=False):
        return self.update_clan_members(tags, save_to_db, return_players=True)

    def update_clan_members(self, tags, save_to_db=False, return_players=False):
        tags = tags if isinstance(tags, list) else [tags]
        tags = [fmt_tag(tag) for tag in tags]
        players = {}
        for c in chunks(tags, CLAN_CHUNK_SIZE):
            rp = self.__update_clan_members(c, save_to_db)
            if return_players:
                players.update(rp)
        return players

    def get_current_season(self):
        today = datetime.today()
        return datetime(today.year, today.month, 1).date()
        # return datetime.strptime("2020-10", "%Y-%m").date()

    @lru_cache()
    def get_or_create_league(self, season=None):
        if not season:
            season = self.get_current_season()
        f = (
            func.DATETIME(LeagueSeason.season) == func.DATETIME(season)
            if db.driver.is_sqlite()
            else LeagueSeason.season == season
        )
        league = LeagueSeason.query.filter(f).one_or_none()

        if not league:
            league = LeagueSeason.from_dict({"season": season})
            db.session.add(league)
            db.session.commit()
        return league

    def get_league_round(self, round_number, league_group):
        lr = league_group.rounds[round_number]
        # print(lr)
        return lr

    def _get_war_logs(self, clan_tags, save_to_db=False):
        t = Timer()
        res = []
        warlogs = []
        d_clan_tags = {}
        for clan_tag in clan_tags:
            # print("clan_tag", clan_tag)
            try:
                wl = self.req.get_war_log(clan_tag)
                warlogs.append(wl)
            except WarLogPrivateException:
                pass
            except FileNotFoundError:
                pass

        # t.ellapsed_print(f" get_war_log append {len(warlogs)}")
        rm = {}
        nwarlogs = []

        for wl in warlogs:
            nl = []
            # pprint(wl)
            first = True
            for i in wl["items"]:
                # print(i)
                if "tag" not in i["opponent"] or i["result"] is None:
                    # print("NOT FOUND")
                    continue  ## a defunc clan or war
                i["state"] = "warEnded"
                i["war_log"] = True
                if first:
                    d_clan_tags[i["clan"]["tag"]] = {
                        "tag": i["clan"]["tag"],
                        "name": i["clan"]["name"],
                    }
                    first = False
                d_clan_tags[i["opponent"]["tag"]] = {
                    "tag": i["opponent"]["tag"],
                    "name": i["opponent"]["name"],
                }
                nl.append(i)

            # pprint(nl)
            # wl["items"] = nl
            nwarlogs.append(nl)

        self.dbcont._insert_players_or_clans(
            d_clan_tags, Clan, ClanNameHistory, ClanDescriptionHistory,
        )
        # t.ellapsed_print(" war logs append ")

        return self._get_wars(nwarlogs, save_to_db=save_to_db)

    @timeit
    def get_war_log(self, clan_tag, save_to_db=False):
        assert isinstance(clan_tag, str)
        return self._get_war_logs([clan_tag], save_to_db=save_to_db)

    @timeit
    def get_war_logs(self, clan_tags, save_to_db=False):
        wls = []
        for c in chunks(clan_tags, CLAN_CHUNK_SIZE):
            wls.extend(self._get_war_logs(c, save_to_db=save_to_db))
        return wls

    def get_league_season(self, season=None):
        if not season:
            season = self.get_current_season()
        ls = self.get_or_create_league(season)
        return ls


    def get_league_group(self, clan_tag, season=None, get_war_log=False, get_wars=False, save_to_db=False):
        # t.ellapsed_print("get_league_group")
        # t = Timer()
        lgid = self._get_league_group(
            clan_tag, season, get_war_log=get_war_log, save_to_db=save_to_db
        )
        # print("lgid=", lgid)
        if not lgid:
            return None
        lg = LeagueGroup.query.filter(LeagueGroup.id == lgid).one_or_none()
        # print("lg = ", lg)
        if lg is None:
            return None
        needs_commit = False
        # t.ellapsed_print("  got league")

        if lg and get_wars:
            # t.ellapsed_print("  lg")
            # print(" ", len(lg.rounds))
            for nround, r in enumerate(lg.rounds):
                # t.ellapsed_print("    r", nround)
                # print("  r", r)
                for wt in r.war_tags:
                    dbw = self.dbcont.get_war_from_war_tag(wt.war_tag)
                    if not dbw:
                        # t.ellapsed_print(f" new war-")
                        ## New War
                        try:
                            w = self.get_war_from_war_tag(wt.war_tag, r.id)
                        except NotInWarException:
                            continue
                        r.wars.append(w)
                        db.session.add(w)
                        db.session.flush()
                        wt.war_id = w.id
                        needs_commit = True
                    elif dbw.war_status == WarState.war_ended:
                        # t.ellapsed_print(f" ended war-, {dbw.clan1_tag}, {dbw.clan2_tag}, war_id={wt.war_id}, lr={wt.league_round_id}")
                        ## We have an ended war, just update the id if needed
                        if wt.war_id is None:
                            wt.war_id = dbw.id
                            db.session.merge(wt)
                            needs_commit = True
                        elif dbw.league_round_id is None: ### the league round id is not being set correctly for ended wars.. is this the reason???
                            w = self.get_war_from_war_tag(wt.war_tag, r.id)
                            w._copy_ids_from(dbw)
                            r._replace_insert_war(w)
                            db.session.merge(w)
                            needs_commit = True
                        # else:
                            # print("get to skip ", dbw.league_round_id, dbw.clan1_tag, dbw.clan2_tag)

                    else:
                        # t.ellapsed_print(f" old fwar- {dbw.clan1_tag}, {dbw.clan2_tag}")
                        ## Old war that hasn't ended. Merge data
                        w = self.get_war_from_war_tag(wt.war_tag, r.id)
                        w._copy_ids_from(dbw)
                        r._replace_insert_war(w)
                        db.session.merge(w)

                        if wt.war_id is None:
                            wt.war_id = w.id
                            db.session.merge(wt)

                        needs_commit = True
            if needs_commit:
                db.session.commit()
        return lg

    def _get_league_group(self, clan_tag, season=None, get_war_log=False, save_to_db=False):
        # t.ellapsed_print("_get_league_group")
        clan_tag = fmt_tag(clan_tag)
        ls = self.get_league_season(season)
        # t = Timer()
        ### Avoid additional req if we know the clan isn't in league
        if self.dbcont.in_league(clan_tag) == False:
            return None
        # t.ellapsed_print("1")
        ob = self.dbcont.get_league_group(clan_tag, ls.id)
        # t.ellapsed_print(f"2 -- {ob}, {ob.has_all_war_tags() if ob else None}")
        # t.ellapsed_print("#################  ", ob)
        needs_commit = False
        if ob and ob.has_all_war_tags():
            lg = ob
            # t.ellapsed_print("has all wartags")
        else:
            # t.ellapsed_print("3")
            try:
                lgdict = self.req.get_league_group(clan_tag)
            except FileNotFoundError:
                self.dbcont.set_in_league(clan_tag, ls.id, False)
                return None
            except NotInWarException:
                self.dbcont.set_in_league(clan_tag, ls.id, False)
                return None
            # t.ellapsed_print("4")

            lg = LeagueGroup(ls.id, **lgdict)
            # t.ellapsed_print("5")
            if ob and ob.league_id:
                lg.league_id = ob.league_id
            if not lg.league_id:
                # t.ellapsed_print("6")
                for r in lg.rounds:
                    r.war_tags[:] = [wt for wt in r.war_tags if not wt.exists()]
                clan = self.get_clan(clan_tag, save_to_db=save_to_db)
                lg.league_id = clan.war_league
            if ob:
                # t.ellapsed_print("7")
                lg._copy_ids_from(ob)
                # t.ellapsed_print("8")

                db.session.merge(lg)
                needs_commit = True
            else:
                # t.ellapsed_print("inserting players clans")
                ps = {}
                # WarTag._get_tags(lg.)
                for c in lg.clans or []:
                    for p in c.members or []:
                        ps[p.tag] = {"tag": p.tag, "name": p.name}
                # t.ellapsed_print(" 2 inserting players clans")
                self.dbcont._insert_players_or_clans(ps, Player, PlayerNameHistory)
                # print("   c==", c)
                self.dbcont._insert_players_or_clans(
                    {c.tag: c.as_dict() for c in lg.clans},
                    Clan,
                    ClanNameHistory,
                    ClanDescriptionHistory,
                )
                #t.ellapsed_print(" 3 inserting players clans")
                db.session.add(lg)
                needs_commit = True

        if needs_commit:
            # t.ellapsed_print("going to commit")
            db.session.commit()
            # t.ellapsed_print("committed")

        # if ob:
        #     for i, r in enumerate(lg.rounds):
        #         if i < len(ob.rounds):
        #             lg.rounds[i].wars = ob.rounds[i].wars

        # players = []
        return lg.id

    def get_war_from_war_tag(self, war_tag, round_id, save_to_db=False):
        wdict = self.req.get_league_war(war_tag)
        # print("3333", wdict)
        wdict["league_round_id"] = round_id
        wdict["war_tag"] = war_tag
        return self._get_war(wdict, save_to_db=save_to_db)

    def get_current_war(self, tag, save_to_db=False):
        return self._get_war(self.req.get_current_war(tag), save_to_db=save_to_db)

    @timeit
    def _get_wars(self, list_wlogs, save_to_db=False):
        wars = []
        t = Timer()
        wars = [
            War(**wdict)
            for clan_warlog in list_wlogs
            for wdict in clan_warlog
            if wdict.get("state", None) != "notInWar"
        ]
        # for clan_warlog in list_wlogs:
        #     # print("   ---", len(clan_warlog), len(list_wlogs))
        #     for wdict in clan_warlog:
        #         try:
        #             wars.append(War(**wdict))
        #         except NotInWarException:
        #             pass
        # t.ellapsed_print(" appended", len(wars))
        obs = War._get_wars(wars)

        # t.ellapsed_print(" got_wars", len(obs))
        assert len(obs) <= len(wars)
        # print(len(obs), len(list_wlogs))

        if save_to_db:
            ## if we have an old war and it wasn't already inserted(war ended)
            ## put in the new info
            if obs:
                dobs = {x._eq_key_(): x for x in obs}
                not_found = {x._eq_key_(): x for x in wars if x._eq_key_() not in dobs}
                # t.ellapsed_print(" have obs", len(obs))
                # print(obs)
                # print("have old war!!", ob)
                for ob in dobs.values():
                    if ob.state != WarState.war_ended:
                        sd = war._copy_ids_from(ob)
                        for c, lo in sd.items():
                            for o in lo:
                                db.session.delete(o)
                        db.session.merge(war)
                        # t.ellapsed_print("     merged", len(obs))
                db.session.add_all(not_found.values())
                db.session.commit()
            else:
                # t.ellapsed_print(" adding")
                db.session.add_all(wars)
                db.session.commit()
                # t.ellapsed_print(" committed")

        return wars

    def get_unfinished_wars(self):
        return self.dbcont.get_unfinished_wars()

    def _get_war(self, wdict, save_to_db=False):
        try:
            war = War(**wdict)
        except NotInWarException:
            return None
        ob = self.dbcont.get_war(war)

        if save_to_db:
            ## if we have an old war and it wasn't already inserted(war ended)
            ## put in the new info
            if ob:
                # print("have old war!!", ob)
                if ob.state != WarState.war_ended:
                    sd = war._copy_ids_from(ob)
                    for c, lo in sd.items():
                        for o in lo:
                            db.session.delete(o)
                    db.session.merge(war)
                    db.session.commit()
            else:
                db.session.add(war)
                db.session.commit()

        return war

    def __update_clan_members(self, tags, save_to_db=False):
        players = {}
        clan_dicts = {}
        ### get players from clan tag
        for tag in tags:
            clandict = self.req.get_clan_members(tag)
            clan_dicts[tag] = clandict
            for pdict in clandict["items"]:
                players[pdict["tag"]] = Player.odict_from_dict(pdict)
        if save_to_db:
            self.dbcont.insert_players(players)

        temp = Player.get_last_histories(list(players.keys()))
        dict_hists = {e.tag: e for e in temp}
        # temp = self.dbcont.get_last_names(PlayerHistory, list(players.keys()))
        # dict_names = {e.tag: e for e in temp}
        now = datetime.utcnow()
        new_entries = []

        ###
        for tag in tags:
            clandict = clan_dicts[tag]
            for pdict in clandict["items"]:
                ph = PlayerHistory(**pdict)
                lh = dict_hists.get(ph.tag, None)
                if lh:
                    lh.entry_type = EntryType.from_clan_members
                if not lh or lh != ph:
                    new_entries.append(ph)
                elif (lh.last_check - now).total_seconds() >= MIN_TIME_TO_UPDATE:
                    lh.last_check = now
                    if save_to_db:
                        lh.update()  ## update the check time
        # print("new and update=", len(new_entries), len(updates))
        if save_to_db:
            db.session.bulk_save_objects(new_entries)
            db.session.commit()
        return players

    def _get_player_or_clan(
        self,
        pdict,
        main_cls,
        hist_cls,
        name_hist_cls,
        desc_cls=None,
        entry_type=None,
        get_war_log=False,
        save_to_db=False,
    ):
    ### TODO get_war_log not implemented
        # print("_get_player_or_clan")

        # print(type(pdict), str(entry_type))
        if entry_type:
            pdict["entry_type"] = entry_type.value
        p = main_cls(**pdict)
        ph = hist_cls(**pdict)
        nh = name_hist_cls(**pdict)
        dh = None if not desc_cls else desc_cls(**pdict)
        # print(" \n# ----\n", p, ph, nh, dh)
        if save_to_db:
            ## We have a new player/clan
            if not p.exists():
                db.session.add(p)
                db.session.flush()
                db.session.add(ph)
                db.session.add(nh)
                if dh:
                    db.session.add(dh)
                db.session.flush()
                if dh:
                    ph.description_id = dh.id
            else:
                if dh:
                    cdh_id = desc_cls.get_id(dh.tag, dh.description)
                    # print("    ######## ", cdh_id)
                    if cdh_id is None:
                        # print("ADDING THE CDH")
                        db.session.add(dh)
                        db.session.flush()
                        cdh_id = dh.id
                    ph.description_id = cdh_id
                if nh:
                    nh_id = name_hist_cls.get_id(nh.tag, nh.name)
                    # print("    ######## ", nh_id)
                    if nh_id is None:
                        # print("ADDING THE NH")
                        db.session.add(nh)
                        p.name = nh.name
                        p.merge()

                lh = p.last_history()
                # print(type(lh), type( ph))
                if lh is None or lh != ph:
                    # print("Adding for some reason")
                    db.session.add(ph)
                else:
                    # print("going to update")
                    lh.last_check = datetime.utcnow()
                    lh.update()  ## update the check time
            db.session.commit()

        if dh:
            ph._description = dh.description
            ph._description_obj = dh
            p.descriptions.append(dh)
        p.names.append(nh)
        p.history.append(ph)
        # print(" -------- _get_player_or_clan")
        return p

    def get_player(self, tag, save_to_db=False):
        d = self.req.get_player(tag)
        return self._get_player_or_clan(
            d, Player, PlayerHistory, PlayerNameHistory, save_to_db=save_to_db
        )

    def get_clan(self, tag, get_war_log=False, save_to_db=False):
        d = self.req.get_clan(tag)
        return self._get_player_or_clan(
            d,
            Clan,
            ClanHistory,
            ClanNameHistory,
            ClanDescriptionHistory,
            entry_type=EntryType.from_clan,
            get_war_log=get_war_log,
            save_to_db=save_to_db,
        )

    def get_clan_members(self, tag, save_to_db=False):
        d = self.req.get_clan_members(tag)
        c = self._get_player_or_clan(
            d,
            Clan,
            ClanHistory,
            ClanNameHistory,
            ClanDescriptionHistory,
            entry_type=EntryType.from_clan_members,
            save_to_db=save_to_db,
        )
        return c.members


if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

    if os.path.exists(dotenv_file):
        import dotenv

        print(f"Loading dotenv '{dotenv_file}'")
        dotenv.load_dotenv(dotenv_file)

