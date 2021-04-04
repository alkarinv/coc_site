import collections
import re
from datetime import date, datetime, timedelta
from enum import IntEnum

import pandas as pd
from sqlalchemy import (
    DDL,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    and_,
    or_,
)
from sqlalchemy.event import listen
from sqlalchemy.orm import backref, relationship
from sqlalchemy.schema import Table
from sqlalchemy.sql import func

from models.db import Base as DBBase
from models.db import driver
from models.db import session as dbsession
from models.exceptions import NotInWarException, TagNotFoundException
from models.timer import timeit
from models.utils import chunks, fmt_tag

_camel2snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")
# "20200729T194845.000Z"
_DATE_STR_FORMAT_ = "%Y%m%dT%H%M%S.%fZ"
_IN_TAG_CHUNK_SIZE_ = 1000



_clans_players_association_table = Table(
    "association",
    DBBase.metadata,
    Column("clan_tag", String(12), ForeignKey("clan.tag")),
    Column("player_tag", String(12), ForeignKey("player.tag")),
)

_clans_wars_association_table = Table(
    "clans_2_wars",
    DBBase.metadata,
    Column("clan_tag", String(12), ForeignKey("clan.tag")),
    Column("war_id", Integer, ForeignKey("war.id")),
)

def camel_to_snake(name):
    return _camel2snake_pattern.sub("_", name).lower()


def _get_last(hcls, tags):
    subq = (
        dbsession.query(hcls.id, func.max(hcls.last_check).label("most_recent"))
        .filter(hcls.tag.in_(tags))
        .group_by(hcls.tag)
        .subquery()
    )
    q = (dbsession.query(hcls).join(subq, hcls.id == subq.c.id)).all()
    return q


class WarResult(IntEnum):
    tie = 0
    clan1_win = 1
    clan2_win = 2

class WarFrequency(IntEnum):
    never = 0
    always = 1
    onceperweek = 2
    twiceaweek = 3
    morethanonceperweek = 4
    lessthanonceperweek = 5
    unknown = 6

    @staticmethod
    def from_str(label):
        try:
            return WarFrequency[label.lower()]
        except:
            up = label.lower()
            if "twice" in up:
                return WarFrequency.twiceaweek
            raise


class ClanEntryType(IntEnum):
    closed = 0
    open = 1
    invite_only = 2

    @staticmethod
    def from_str(label):
        try:
            return ClanEntryType[label.lower()]
        except:
            up = label.lower()
            if up in ("inviteonly"):
                return ClanEntryType.invite_only
            # elif up in ("INWAR"):
            #     return WarState.IN_WAR
            # elif up in ("WARENDED"):
            #     return WarState.WAR_ENDED
            raise


class WarLeague(IntEnum):
    unranked = 48000000
    bronze3 = 48000001
    bronze2 = 48000002
    bronze1 = 48000003
    silver3 = 48000004
    silver2 = 48000005
    silver1 = 48000006
    gold3 = 48000007
    gold2 = 48000008
    gold1 = 48000009
    crystal3 = 48000010
    crystal2 = 48000011
    crystal1 = 48000012
    master3 = 48000013
    master2 = 48000014
    master1 = 48000015
    champ3 = 48000016
    champ2 = 48000017
    champ1 = 48000018


class League(IntEnum):
    unranked = 29000000
    bronze3 = 29000001
    bronze2 = 29000002
    bronze1 = 29000003
    silver3 = 29000004
    silver2 = 29000005
    silver1 = 29000006
    gold3 = 29000007
    gold2 = 29000008
    gold1 = 29000009
    crystal3 = 29000010
    crystal2 = 29000011
    crystal1 = 29000012
    master3 = 29000013
    master2 = 29000014
    master1 = 29000015
    champ3 = 29000016
    champ2 = 29000017
    champ1 = 29000018
    titan3 = 29000019
    titan2 = 29000020
    titan1 = 29000021
    legend = 29000022


class EntryType(IntEnum):
    from_player = 0
    from_clan = 1
    from_clan_members = 2


class WarState(IntEnum):
    preparation = 0
    in_war = 1
    war_ended = 2


    @staticmethod
    def from_str(label):
        try:
            return WarState[label.lower()]
        except:
            up = label.lower()
            if up in ("preparation"):
                return WarState.preparation
            elif up in ("inwar"):
                return WarState.in_war
            elif up in ("warended"):
                return WarState.war_ended
            raise


class WarType(IntEnum):
    normal = 0
    friendly = 1
    league = 2

    def __str__(self):
        return self.name


class WarAttack(DBBase):
    __tablename__ = "war_attack"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    war = relationship("War", back_populates="attacks")
    war_player = relationship("WarPlayer", back_populates="attacks")
    war_id = Column(Integer, ForeignKey("war.id"), index=True)
    war_player_id = Column(Integer, ForeignKey("war_player.id"), index=True)
    attacker_tag = Column(String(12), ForeignKey("player.tag"), nullable=False, index=True)
    defender_tag = Column(String(12), ForeignKey("player.tag"), nullable=False, index=True)
    stars = Column(Integer)
    destruction_percentage = Column(Integer)
    order = Column(Integer)

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, camel_to_snake(attr), value)

    def __str__(self):
        return f"({self.attacker_tag} -> {self.defender_tag} ({self.destruction_percentage}%) {self.stars} star(s))"

    def __repr__(self):
        return str(self)

    @property
    def attacker_th(self):
        return self.war_members.town_hall_level

    @property
    def defender_th(self):
        p = self.war.get_player(self.defender_tag)
        return p.town_hall_level if p else None

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, WarAttack):
            raise NotImplemented
        self.id = from_other.id
        return {}


class WarPlayer(DBBase):
    __tablename__ = "war_player"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    war_clan_id = Column(Integer, ForeignKey("war_clan.id"), index=True)
    parent = relationship("WarClan", back_populates="members")
    tag = Column("tag", String(12), ForeignKey("player.tag"), nullable=False, index=True)
    town_hall_level = Column(Integer)
    map_position = Column(Integer)
    attacks = relationship("WarAttack", lazy="joined", back_populates="war_player")

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, WarPlayer):
            raise NotImplemented
        if from_other.id:
            self.id = from_other.id
        should_delete = collections.defaultdict(list)
        if not recursive:
            return should_delete
        ma = {f"{e.attacker_tag}:{e.defender_tag}": e for e in self.attacks}
        oa = {f"{e.attacker_tag}:{e.defender_tag}": e for e in from_other.attacks}
        for tag, a in oa.items():
            if tag in ma:
                ma[tag]._copy_ids_from(a)
            else:
                should_delete[WarAttack].append(a)
        return should_delete

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            if attr == "bestOpponentAttack":
                continue
            if attr.lower() == "townhalllevel":  # this is sometimes mispelled from api
                setattr(self, "town_hall_level", value)
            elif attr == "attacks":
                for a in kwargs["attacks"]:
                    self.attacks.append(WarAttack(**a))
            else:
                setattr(self, camel_to_snake(attr), value)

    @property
    def best_opponent_attack(self):
        war_attacks = self.parent.parent.get_defenses(self.tag)
        best = None
        for wa in war_attacks:
            if not best:
                best = wa
            else:
                if wa.stars > best.stars or (
                    wa.stars == best.stars
                    and wa.destruction_percentage > best.destruction_percentage
                ):
                    best = wa

        return best

    def __str__(self):
        return f"({self.tag} {len(self.attacks)})"

    def __repr__(self):
        return str(self)


class LeaguePlayer(DBBase):
    __tablename__ = "league_player"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag = Column("tag", String(12), ForeignKey("player.tag"), nullable=False, index=True)
    town_hall_level = Column(Integer)
    league_clan_id = Column("league_clan_id", Integer, ForeignKey("league_clan.id"), index=True)
    parent = relationship("LeagueClan", back_populates="members")
    league_season_id = Column("league_season_id", Integer, ForeignKey("league_season.id"), index=True)
    in_league = Column(Boolean, default=True)

    def __init__(self, league_season_id, **kwargs):
        for attr, value in kwargs.items():
            if attr == "townhallLevel":  # this is sometimes mispelled from api
                attr = "townHallLevel"
            setattr(self, camel_to_snake(attr), value)
        self.league_season_id = league_season_id

    @staticmethod
    def get_clan_name(player_tag):
        try:
            clan_id = (
                dbsession.query(LeaguePlayer.league_clan_id)
                .filter(LeaguePlayer.tag == player_tag)
                .all()[0][0]
            )
            clan_tag = dbsession.query(LeagueClan.tag).filter(LeagueClan.id == clan_id).scalar()
            return dbsession.query(Clan.name).filter(Clan.tag == clan_tag).scalar()
        except:
            return "unknown"

    @staticmethod
    def get_clan_names(player_tags):
        # print("######@@@@@@@@@@@@@@@@@@@@@@##\n", type(player_tag), "####00000000000#####\n")
        r = []
        for c in chunks(player_tags, _IN_TAG_CHUNK_SIZE_):
            # print(len(c))
            myl = list(c)
            d = collections.OrderedDict([(x, None) for x in myl])
            # print("stenwentes", len(d), len(myl))
            clan_ids = (
                dbsession.query(LeaguePlayer.tag, LeaguePlayer.league_clan_id)
                .filter(LeaguePlayer.tag.in_(myl))
                .all()
            )
            dclan_ids = {x[0]: x[1] for x in clan_ids}
            for pt in d:
                d[pt] = dclan_ids.get(pt, None)
            # print(len(d), len([x for x in d.values() if x is not None]))
            ids = [x[1] for x in clan_ids]
            clan_tags = (
                dbsession.query(LeagueClan.id, LeagueClan.tag).filter(LeagueClan.id.in_(ids)).all()
            )
            dclan_tags = {x[0]: x[1] for x in clan_tags}
            for pt, clan_id in d.items():
                d[pt] = dclan_tags.get(clan_id, None)
            # print(len(d), len([x for x in d.values() if x is not None]))
            cnames = (
                dbsession.query(Clan.tag, Clan.name).filter(Clan.tag.in_(dclan_tags.values())).all()
            )
            dcnames = {x[0]: x[1] for x in cnames}
            for pt, clan_tag in d.items():
                if clan_tag and clan_tag in dcnames:
                    d[pt] = dcnames[clan_tag]
            # print(len(d), len([x for x in d.values() if x is not None]))
            r.extend([d.get(x, None) for x in myl])
        return r

    @staticmethod
    def get_clan_tags(player_tags):
        # print("######@@@@@@@@@@@@@@@@@@@@@@##\n", type(player_tag), "####00000000000#####\n")
        r = []
        for c in chunks(player_tags, _IN_TAG_CHUNK_SIZE_):
            # print(len(c))
            myl = list(c)
            d = collections.OrderedDict([(x, None) for x in myl])
            # print("stenwentes", len(d), len(myl))
            clan_ids = (
                dbsession.query(LeaguePlayer.tag, LeaguePlayer.league_clan_id)
                .filter(LeaguePlayer.tag.in_(myl))
                .all()
            )
            dclan_ids = {x[0]: x[1] for x in clan_ids}
            for pt in d:
                d[pt] = dclan_ids.get(pt, None)
            # print(len(d), len([x for x in d.values() if x is not None]))
            ids = [x[1] for x in clan_ids]
            clan_tags = (
                dbsession.query(LeagueClan.id, LeagueClan.tag).filter(LeagueClan.id.in_(ids)).all()
            )
            dclan_tags = {x[0]: x[1] for x in clan_tags}
            for pt, clan_id in d.items():
                d[pt] = dclan_tags.get(clan_id, None)
            # print(len(d), len([x for x in d.values() if x is not None]))
            r.extend([d.get(x, None) for x in myl])
        return r


class LeagueClan(DBBase):
    __tablename__ = "league_clan"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag = Column("tag", String(12), ForeignKey("clan.tag"), nullable=False)
    league_group_id = Column("league_group_id", Integer, ForeignKey("league_group.id"), index=True)
    league_season_id = Column("league_season_id", Integer, ForeignKey("league_season.id"), index=True)
    members = relationship("LeaguePlayer", lazy="joined", back_populates="parent")
    in_league = Column(Boolean, default=True)
    parent = relationship("LeagueGroup", back_populates="clans")

    def __init__(self, league_season_id, **kwargs):
        self.league_season_id = league_season_id
        for attr, value in kwargs.items():
            if attr == "members":
                continue
            setattr(self, camel_to_snake(attr), value)
        if "members" in kwargs:
            for m in kwargs["members"]:
                self.members.append(LeaguePlayer(league_season_id, **m))

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, LeagueClan):
            raise NotImplemented
        if from_other.id:
            self.id = from_other.id
        self.league_group_id = from_other.league_group_id
        return {}


class WarClan(DBBase):
    __tablename__ = "war_clan"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    war_id = Column("war_id", Integer, ForeignKey("war.id"), index=True)
    parent = relationship("War", back_populates="clans")
    tag = Column("tag", String(12), ForeignKey("clan.tag"), index=True)
    clan_level = Column(Integer)
    attacks = Column(Integer)
    stars = Column(Integer)
    destruction_percentage = Column(Float)
    exp_earned = Column(Integer)

    members = relationship("WarPlayer", lazy="joined", back_populates="parent")

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            if attr == "members":
                continue
            setattr(self, camel_to_snake(attr), value)
        for m in kwargs.get("members", []):
            self.members.append(WarPlayer(**m))

    def __str__(self):
        return f"(WarClan {self.name} #atks={self.attacks} stars={self.stars} d%={self.destruction_percentage}"

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, WarClan):
            raise NotImplemented
        if from_other.id:
            self.id = from_other.id
        should_delete = collections.defaultdict(list)
        if not recursive:
            return should_delete

        myp = {p.tag: p for p in self.members}
        op = {p.tag: p for p in from_other.members}
        for tag, p in op.items():
            if tag in myp:
                should_delete.update(myp[tag]._copy_ids_from(p))
            else:
                should_delete[WarPlayer].append(p)
        return should_delete


class WarTag(DBBase):
    __tablename__ = "war_tag"
    __table_args__ = {"extend_existing": True}

    war_tag = Column(String(12), primary_key=True)
    # war_tag = Column(String(12), ForeignKey("war.war_tag"), primary_key=True)
    league_round_id = Column("league_round_id", Integer, ForeignKey("league_round.id"), index=True)
    # war_id = Column("war_id", Integer, ForeignKey("war.id"), index=True)
    war_id = Column("war_id", Integer) ## mysql doesn't like ForeignKeys and indexing on potential NULL
    parent = relationship("LeagueRound", back_populates="war_tags")

    def __init__(self, war_tag):
        self.war_tag = war_tag

    def __str__(self):
        return f"(WarTag {self.war_tag}, r={self.league_round_id}, wid={self.war_id})"

    def __repr__(self):
        return str(self)

    @staticmethod
    def _get_tags(league_round_id):
        return dbsession.query(WarTag).filter(WarTag.league_round_id == league_round_id).all()


    def exists(self):
        return dbsession.query(WarTag.war_tag).filter(WarTag.war_tag == self.war_tag).scalar()

class LeagueRound(DBBase):
    __tablename__ = "league_round"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    league_group_id = Column("league_group_id", Integer, ForeignKey("league_group.id"), index=True)
    round = Column("round", Integer)
    parent = relationship("LeagueGroup", back_populates="rounds")
    league_season_id = Column("season_id", Integer, ForeignKey("league_season.id"), index=True)

    war_tags = relationship("WarTag", back_populates="parent")
    wars = relationship("War", lazy="joined", back_populates="parent")

    def __init__(self, league_season_id, round, **kwargs):
        self.league_season_id = league_season_id
        self.round = round
        for attr, value in kwargs.items():
            if attr == "warTags":
                for i, war_tag in enumerate(value):
                    if war_tag == "#0":
                        continue
                    wt = WarTag(war_tag)
                    self.war_tags.append(wt)

    def get_war(self, war_tag):
        for w in self.wars:
            if w.war_tag == war_tag:
                return w
        return None

    def has_war(self, war_tag):
        for w in self.wars:
            if w.war_tag == war_tag:
                return True
        return False

    def _replace_insert_war(self, war):
        index = None
        for i, w in enumerate(self.wars):
            if w.war_tag == war.war_tag:
                index = i
                break
        if index is not None:
            self.wars[index] = w

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, LeagueRound):
            raise NotImplemented
        if from_other.id:
            self.id = from_other.id
        if from_other.league_group_id:
            self.league_group_id = from_other.league_group_id
        if not recursive:
            return {}
        should_delete = collections.defaultdict(list)
        myo = {o._eq_key_(): o for o in self.wars}
        oo = {o._eq_key_(): o for o in from_other.wars}
        # self.wars = from_other.wars
        for key, o in oo.items():
            if key in myo:
                should_delete.update(myo[tag]._copy_ids_from(o))
            else:
                should_delete[War].append(o)
        myo = {o.war_tag: o for o in self.war_tags}
        oo = {o.war_tag: o for o in from_other.war_tags}
        for key, o in oo.items():
            if key in myo:
                # should_delete.update(myo[tag]._copy_ids_from(o))
                myo[key].league_round_id = o.league_round_id
        return should_delete

    def __str__(self):
        return f"(LeagueRound {self.id} {self.war_tags})"


class LeagueGroup(DBBase):
    ### TODO state doesn't really mean much in group as it doesn't always reflect db status
    __tablename__ = "league_group"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    state = Column(Integer)
    league_id = Column(Integer)
    league_season_id = Column("season_id", Integer, ForeignKey("league_season.id"), index=True)
    clans = relationship("LeagueClan")
    rounds = relationship("LeagueRound", back_populates="parent")
    parent = relationship("LeagueSeason", back_populates="groups")

    def __init__(self, league_season_id, **kwargs):
        self.league_season_id = league_season_id
        for attr, value in kwargs.items():
            if attr in ("season", "clans", "rounds"):
                pass
            elif attr == "state":
                setattr(self, attr, WarState.from_str(value).value)
            else:
                setattr(self, camel_to_snake(attr), value)

        for c in kwargs["clans"]:
            self.clans.append(LeagueClan(league_season_id, **c))

        for i, r in enumerate(kwargs["rounds"]):
            self.rounds.append(LeagueRound(league_season_id,i, **r))

    def has_all_war_tags(self):
        if len(self.rounds) < 7:
            return False
        c = (
            dbsession.query(WarTag.league_round_id)
            .filter(WarTag.league_round_id.in_([x.id for x in self.rounds]))
            .count()
        )
        return c == 28  # 7 rounds, 4 war_tags per round

    def finished(self):
        if not self.has_all_war_tags():
            return False

        for w in self.rounds[-1].wars:
            if w.state != WarState.war_ended:
                return False
        return True

    def _copy_from(self, other, recursive=True):
        if not isinstance(other, LeagueGroup):
            raise NotImplemented
        copy_attrs = ["state", "league_id", "league_season_id"]
        for attr in copy_attrs:
            setattr(self, attr, getattr(other, attr))
        should_delete = collections.defaultdict(list)

        myo = {e.tag: e for e in self.clans}
        oo = {e.tag: e for e in other.clans}
        for tag, o in oo.items():
            if tag in myo:
                should_delete.update(myo[tag]._copy_from(o))
            else:  # This shouldn't happen for clans (I think)
                should_delete[LeagueClan].append(o)

        for i, r in enumerate(from_other.rounds):
            if i < len(self.rounds):
                should_delete.update(self.rounds[i]._copy_ids_from(r))

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, LeagueGroup):
            raise NotImplemented
        if from_other.id:
            self.id = from_other.id
        if not recursive:
            return {}
        should_delete = collections.defaultdict(list)
        myc = {e.tag: e for e in self.clans}
        oc = {e.tag: e for e in from_other.clans}
        for tag, c in oc.items():
            if tag in myc:
                should_delete.update(myc[tag]._copy_ids_from(c))
            else:  # This shouldn't happen for clans (I think)
                should_delete[LeagueClan].append(c)

        for i, r in enumerate(from_other.rounds):
            if i < len(self.rounds):
                should_delete.update(self.rounds[i]._copy_ids_from(r))
        return should_delete

    def __str__(self):
        return f"(LeagueGroup id:{self.id} season:{self.league_season_id} state:{self.state} clans:{[t.tag for t in self.clans]})"


class LeagueSeason(DBBase):
    __tablename__ = "league_season"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    season = Column(Date, nullable=False)
    flag = Column(Integer)

    groups = relationship("LeagueGroup", back_populates="parent")

    def __str__(self):
        return f"(LeagueSeason {self.id} {self.season} {self.flag})"

    @property
    def members(self):
        return LeaguePlayer.query.filter(LeaguePlayer.league_season_id == self.id).all()

    @property
    def _player_name_map(self):
        f = and_(LeaguePlayer.league_season_id == self.id, LeaguePlayer.tag == Player.tag)
        r = dbsession.query(Player.tag, Player.name).filter(f)
        # print(r)
        return {e[0]: e[1] for e in r.all()}

    @staticmethod
    def get_season(season_str):
        dt = datetime.strptime(season_str, "%Y-%m").date()
        return LeagueSeason.query.filter(LeagueSeason.season == dt).one_or_none()

    @property
    def clans(self):
        return LeagueClan.query.filter(LeagueClan.league_season_id == self.id).all()

    def to_attack_df(self, query_filter=None, limit=None):
        from models.timer import Timer
        t = Timer()
        if query_filter is None:
            query_filter = LeagueGroup.league_season_id == self.id
        else:
            query_filter = and_(query_filter, LeagueGroup.league_season_id == self.id)
        r = (
            dbsession.query(War.id, War.team_size, LeagueRound.round, LeagueGroup.id)
            .join(LeagueRound, War.league_round_id == LeagueRound.id)
            .join(LeagueGroup, LeagueRound.league_group_id == LeagueGroup.id)
            .filter(query_filter)
        )
        if limit:
            r = r.limit(limit)
        lwars = r.all()
        # print(len(lwars))
        t.ellapsed_print(f"lwars {len(lwars)}")
        natks = []

        # print(wars)
        # print(wars[0:10])
        ID = 0
        WAR_ID = 1
        WAR_PLAYER_ID = 2
        ATTACKER_TAG = 3
        DEFENDER_TAG = 4
        for c in chunks(lwars, _IN_TAG_CHUNK_SIZE_):
            wars = {x[0]: x[1:] for x in c}

            # t.ellapsed_print("wars")
            # print("----", len(wars))
            r = dbsession.query(
                WarAttack.id,
                WarAttack.war_id,
                WarAttack.war_player_id,
                WarAttack.attacker_tag,
                WarAttack.defender_tag,
                WarAttack.stars,
                WarAttack.destruction_percentage,
                WarAttack.order,
            ).filter(WarAttack.war_id.in_(wars.keys()))
            atks = r.all()
            # t.ellapsed_print(f"atks {len(atks)}")
            r = dbsession.query(WarPlayer.tag, WarPlayer.town_hall_level).filter(
                WarPlayer.id.in_([x[WAR_PLAYER_ID] for x in atks])
            )
            th_levels = {x[0]: x[1] for x in r.all()}
            # t.ellapsed_print("a tag")

            r = dbsession.query(WarPlayer.tag, WarPlayer.town_hall_level).filter(
                WarPlayer.tag.in_([x[DEFENDER_TAG] for x in atks])
            )
            # print(r, [x[DEFENDER_TAG] for x in atks])
            th_levels.update({x[0]: x[1] for x in r.all()})
            # t.ellapsed_print("d tag")

            # print(atks)
            for a in atks:
                ar = wars.get(a[WAR_ID], -1)
                # print("a=", ar)
                v = list(a)[1:] + [
                    th_levels.get(a[ATTACKER_TAG], -1),
                    th_levels.get(a[DEFENDER_TAG], -1),
                    *ar
                ]
                # print(" v =", v)
                natks.append(v)
            # t.ellapsed_print("append")
            # print(natks)

        a = [
            "war_id",
            "war_player_id",
            "attacker_tag",
            "defender_tag",
            "stars",
            "destruction_percentage",
            "order",
            "attacker_th",
            "defender_th",
            "team_size",
            "round_id",
            "group_id"
        ]
        return pd.DataFrame(data=natks, columns=a,)


class Tournament(DBBase):
    """Currently Unused.

    Args:
        DBBase ([type]): [description]
    """

    __tablename__ = "tournament"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer(), primary_key=True, autoincrement=True)
    wars = relationship("War", lazy="joined", back_populates="tournament")


class War(DBBase):
    __tablename__ = "war"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer(), primary_key=True, autoincrement=True)
    clan1_tag = Column("clan1_tag", String(12), ForeignKey("clan.tag"), index=True)
    clan2_tag = Column("clan2_tag", String(12), ForeignKey("clan.tag"), index=True)
    # mysql doesn't like the potential None foreign keys so let's try removing these for a bit
    # clan1_tag = Column("clan1_tag", String(12), ForeignKey("clan.tag"), index=True)
    # clan2_tag = Column("clan2_tag", String(12), ForeignKey("clan.tag"), index=True)
    war_type = Column(Integer)
    state = Column(Integer)
    team_size = Column(Integer)
    preparation_start_time = Column(DateTime(timezone=True))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True), nullable=False)

    war_result = Column(Integer)
    destruction_percentage = Column(Float)
    clans = relationship("WarClan", back_populates="parent")
    attacks = relationship("WarAttack", back_populates="war")

    # war_tag = Column(String(12), unique=True, index=True)
    war_tag = Column(String(12), unique=True)##mysql testing
    league_round_id = Column("league_round_id", Integer, ForeignKey("league_round.id"), index=True)

    parent = relationship("LeagueRound", back_populates="wars")

    tournament_id = Column("tournament_id", Integer, ForeignKey("tournament.id"), index=True)
    tournament = relationship("Tournament", back_populates="wars")

    __equality_attrs__ = set(["clan1_tag", "clan2_tag", "end_time"])

    def __init__(self, **kwargs):
        self.war_result = None
        if kwargs["state"] == "notInWar":
            raise NotInWarException(**kwargs)
        home_team = kwargs["clan"]["tag"] <= kwargs["opponent"]["tag"]
        for attr, value in kwargs.items():
            if attr.endswith("Time"):
                setattr(self, camel_to_snake(attr), datetime.strptime(value, _DATE_STR_FORMAT_))
            elif attr == "result":
                if value=="tie":
                    self.war_result = WarResult.tie.value
                else:
                    ## home(clan) and win .. clan1_win
                    ## home(clan) and lose... clan2_win
                    ## home(opponent) and win... clan2_win
                    ## home(opponent) and lose ... clan1_win
                    self.war_result = WarResult.clan1_win.value if (value=="win" and home_team or value=="lose" and not home_team) else WarResult.clan2_win.value
            elif attr == "state":
                setattr(self, attr, WarState.from_str(value).value)
            else:
                setattr(self, camel_to_snake(attr), value)
        self.clans.append(WarClan(** (kwargs["clan"] if home_team else kwargs["opponent"])))
        self.clans.append(WarClan(** (kwargs["opponent"] if home_team else kwargs["clan"])))
        self.clan1_tag = self.clans[0].tag
        self.clan2_tag = self.clans[1].tag
        self.attacks = self.attacks3

        if kwargs.get("isLeague", False):
            self.war_type = WarType.league.value
        elif kwargs.get("war_log", False) or (self.start_time - self.preparation_start_time) == timedelta(hours=23):
            self.war_type = WarType.normal.value
        else:
            self.war_type = WarType.friendly.value
        if self.war_result is not None and self.state == WarState.war_ended:
            self._calc_result()


    def _eq_key_(self):
        return ":".join([str(getattr(self, k)) for k in self.__equality_attrs__])

    @property
    def attacks3(self):
        ps = self.clan1.members + self.clan2.members
        # print(ps)
        return [a for p in ps if p.attacks for a in p.attacks]


    @staticmethod
    def get_all_wars():
        return dbsession.query(War).order_by(War.end_time).all()

    @staticmethod
    @timeit
    def _get_wars(wars):
        if not wars:
            return []
        fs = []
        clans = set()
        for w in wars:
            # print(w.end_time, w.clan1_tag, w.clan2_tag, w.state)
            clans.add(w.clan1_tag)
            f = func.DATETIME(War.end_time) == func.DATETIME(w.end_time)  if driver.is_sqlite() else War.end_time == w.end_time
            fs.append(
                and_(
                    War.clan1_tag==w.clan1_tag,
                    War.clan2_tag==w.clan2_tag,
                    f
                ))

        q = (dbsession.query(War).filter(War.clan1_tag.in_(clans)).filter(or_(*fs)))
        ws = q.all()

        return ws

    @staticmethod
    def _get_war_from_war_tag(war_tag):
        return dbsession.query(War).filter(War.war_tag == war_tag).one_or_none()

    @staticmethod
    def get_war(clan1_tag, clan2_tag, end_time):
        f = func.DATETIME(War.end_time) == func.DATETIME(end_time) if driver.is_sqlite() else War.end_time == end_time
        return (dbsession.query(War).filter(
            and_(
                    War.clan1_tag==clan1_tag,
                    War.clan2_tag==clan2_tag,
                    f
            )).one_or_none())

    @staticmethod
    def war_exists(clan1_tag, clan2_tag, end_time):
        f = func.DATETIME(War.end_time) == func.DATETIME(w.end_time)  if driver.is_sqlite() else War.end_time == end_time
        r = (dbsession.query(War.id).filter(and_(
                War.clan1_tag==clan1_tag,
                War.clan2_tag==clan2_tag,
                f
            )).one_or_none())
        return True if r else False

    # @property
    # def home_clan(self):
    #     if hasattr(self, "_home_clan_"):
    #         return self._home_clan_
    #     self._home_clan_ = int(self.clans[0].tag < self.clans[1].tag)
    #     return self._home_clan_

    # @home_clan.setter
    # def home_clan(self, value):
    #     self._home_clan = value

    def get_members(self):
        return self.clan1.members + self.clan2.members

    @property
    def clan1(self):
        return self.clans[0] if len(self.clans) else None

    @property
    def clan2(self):
        return self.clans[1] if len(self.clans) > 0 else None

    def _calc_result(self):
        sd = self.clan1.stars - self.clan2.stars
        if sd == 0:
            pd = self.clan1.destruction_percentage - self.clan2.destruction_percentage
            if pd == 0:
                self.war_result = WarResult.tie.value
            else:
                self.war_result = WarResult.clan1_win.value if pd > 0 else WarResult.clan2_win.value
        else:
            self.war_result = WarResult.clan1_win.value if sd > 0 else WarResult.clan2_win.value
        return self.war_result

    @property
    def result(self):
        if self.war_result is not None:
            return self.war_result
        if self.state != WarState.war_ended:
            return None
        self._calc_result()
        return self.war_result

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"('{self.clan1.tag}' vs '{self.clan2.tag}' {self.result} {self.clan1.stars}:{self.clan2.stars}) "

    def get_war_clan_from_player(self, tag, opponent=False):
        if any(p.tag == tag for p in self.clan1.members):
            return self.clan1 if not opponent else self.clan2
        if any(p.tag == tag for p in self.clan2.members):
            return self.clan2 if not opponent else self.clan1
        raise TagNotFoundException(tag)

    def get_war_clan(self, tag, opponent=False):
        if any(p.tag == tag for p in self.clan1.members):
            return self.clan1 if not opponent else self.clan2
        if any(p.tag == tag for p in self.clan2.members):
            return self.clan2 if not opponent else self.clan1
        raise TagNotFoundException(tag)

    def get_player(self, tag):
        pl = next((x for x in self.clan1.members if x.tag == tag), None)
        if pl:
            return pl
        return next((x for x in self.clan2.members if x.tag == tag), None)

    def get_attacks(self, tag):
        wd = self.get_war_clan_from_player(tag)
        atks = []
        for p in wd.members:
            for a in p.attacks:
                if a.attacker_tag == tag:
                    atks.append(a)
        return atks

    def get_defenses(self, tag):
        wd = self.get_war_clan_from_player(tag, True)
        defs = []
        for p in wd.members:
            for a in p.attacks:
                if a.defender_tag == tag:
                    defs.append(a)
        return defs

    @property
    def war_status(self):
        now = datetime.now()
        if self.end_time < now:
            return WarState.war_ended
        elif self.start_time < now:
            return WarState.in_war
        else:
            return WarState.preparation

    def __eq__(self, other):
        if not isinstance(other, War):
            raise NotImplemented
        return all(getattr(self, k) == getattr(other, k) for k in War.__equality_attrs__)

    def _copy_ids_from(self, from_other, recursive=True):
        if not isinstance(from_other, War):
            raise NotImplemented
        if from_other.id:
            self.id = from_other.id
        # print(" what is going on here???? ", self.id, from_other.id)
        # print("               ", self.league_round_id, from_other.league_round_id)
        if from_other.league_round_id:
            self.league_round_id = from_other.league_round_id
        if not recursive:
            return {}
        should_delete = collections.defaultdict(list)
        myc = {e.tag: e for e in self.clans}
        oc = {e.tag: e for e in from_other.clans}
        for tag, c in oc.items():
            if tag in myc:
                should_delete.update(myc[tag]._copy_ids_from(c))
            else:  # This shouldn't happen for clans (I think)
                should_delete[WarClan].append(c)
        return should_delete


class Clan(DBBase):
    __tablename__ = "clan"
    __table_args__ = {"extend_existing": True}
    tag = Column(String(12), primary_key=True)
    name = Column(String(32), index=True)

    history = relationship("ClanHistory", lazy="joined")
    names = relationship("ClanNameHistory", lazy="joined")
    descriptions = relationship("ClanDescriptionHistory", lazy="joined")
    members = relationship("Player", lazy="joined", secondary=_clans_players_association_table)
    wars = relationship("War", lazy="joined", secondary=_clans_wars_association_table)

    def __init__(self, **kwargs):
        self.tag = fmt_tag(kwargs["tag"])
        self.name = kwargs["name"]

    def __str__(self):
        return  f"(Clan {self.tag} '{self.name}')"

    @staticmethod
    def get(tag):
        return dbsession.query(Clan).filter(Clan.tag == tag).one_or_none()


    @staticmethod
    def _get_not_exist(tags):
        fs = dbsession.query(Clan.tag).filter(Clan.tag.in_(tags)).all()
        return set(tags) - set([x[0] for x in fs])

    @staticmethod
    def _odict_from_dict(d):
        return {"tag": d["tag"], "name": d["name"]}

    @staticmethod
    def _get_name_changed(obs):
        if not obs:
            return None
        conditions = (and_(Clan.tag == p["tag"], Clan.name != p["name"]) for p in obs)
        q = Clan.query.filter(or_(*conditions))
        return q.all()

    @staticmethod
    def _get_last_histories(tags):
        return _get_last(ClanHistory, tags)

    @staticmethod
    def _get_last_names(tags):
        return _get_last(ClanNameHistory, tags)

    @staticmethod
    def _get_name_dict(tags):
        clans = dbsession.query(Clan).filter(Clan.tag.in_(tags)).all()
        return {x.tag:x.name for x in clans or []}

    @staticmethod
    def _get_last_descriptions(tags):
        return _get_last(ClanDescriptionHistory, tags)

    @staticmethod
    def _get_descriptions_changed(obs):
        obs = [x for x in obs if "description" in x]
        if not obs:
            return None
        hcls = ClanDescriptionHistory
        conditions = (and_(hcls.tag == x["tag"], hcls.description != x["description"]) for x in obs)
        q = ClanDescriptionHistory.query.filter(or_(*conditions))
        return q.all()
        # hcls = ClanDescriptionHistory
        # q = db.session.query(hcls.id, hcls.description).where(hcls.description.in_(descriptions))
        # qr = {[x[1]:x[0] for x in q.all()]}
        # return {x:qr.get(x, None) for x in descriptions}

    def exists(self):
        return dbsession.query(Clan.tag).filter_by(tag=self.tag).scalar() is not None


    def last_history(self, force_db_load=False):
        if not force_db_load and self.history:
            return self.history[-1]
        lh = (
            ClanHistory.query.filter(ClanHistory.tag == self.tag)
            .order_by(ClanHistory.last_check.desc())
            .first()
        )
        return lh

    def __last_history_attr__(self, attr, default=None, force_db_load=False):
        lh = self.last_history(force_db_load)
        return getattr(lh, attr) if lh else default

    def get_current_league(self, force_db_load=False):
        ### TODO this won't always work as intended. I really need this for updating db stats on league
        lh = self.last_history(force_db_load=force_db_load)
        if not lh:
            return lh
        fom = datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return lh.war_league if lh.last_check >= fom else None





    @property
    def description(self):
        if self.descriptions:
            return self.descriptions[-1].description
        lh = self.last_history()
        return lh.description

    @property
    def war_league(self):
        return self.__last_history_attr__("war_league")

    @property
    def clan_level(self):
        return self.__last_history_attr__("clan_level")

    @property
    def war_win_streak(self):
        return self.__last_history_attr__("war_win_streak")

    @property
    def war_wins(self):
        return self.__last_history_attr__("war_wins")

    @staticmethod
    def _get_last_war(clan_tag):
        subq = (
            dbsession.query(War.id, func.max(War.end_time).label("most_recent"))
            .filter(or_(War.clan1_tag  == clan_tag, War.clan2_tag == clan_tag))
            .group_by(War.clan1_tag, War.clan2_tag)
            .subquery()
        )
        q = (dbsession.query(War).join(subq, War.id == subq.c.id)).one_or_none()
        return q


class ClanHistory(DBBase):
    __tablename__ = "clan_history"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer(), primary_key=True)
    tag = Column(String(12), ForeignKey("clan.tag"), index=True)
    type = Column(Integer)
    description_id = Column(Integer, ForeignKey("clan_description_history.id"), index=True)
    location = Column(Integer)
    clan_level = Column(Integer)
    clan_points = Column(Integer)
    clan_versus_points = Column(Integer)
    required_trophies = Column(Integer)
    war_frequency = Column(Integer)
    war_win_streak = Column(Integer)
    war_wins = Column(Integer)
    war_ties = Column(Integer)
    war_losses = Column(Integer)
    is_war_log_public = Column(Boolean)
    war_league = Column(Integer)
    members = Column(Integer)

    insert_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_check = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __equality_attrs__ = set(
        [
            "tag",
            "location",
            "clan_level",
            "clan_points",
            "clan_versus_points",
            "required_trophies",
            "war_frequency",
            "war_win_streak",
            "war_wins",
            "is_war_log_public",
            "war_league",
            "members",
            "description_id",
        ]
    )

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            if attr in (["memberList", "labels", "description"]):
                continue
            # print(" -- ", attr, len(value) if isinstance(value, str) else 0, value)
            if attr == "warLeague":
                value = WarLeague(value["id"]).value
            elif attr == "location":
                value = value["id"]
            elif attr == "warFrequency":
                value = WarFrequency.from_str(value).value
            elif attr == "type":
                value = ClanEntryType.from_str(value).value
            setattr(self, camel_to_snake(attr), value)
        self.tag = fmt_tag(self.tag)

    def __eq__(self, other):
        if not isinstance(other, ClanHistory):
            raise NotImplemented
        # for k in self.__equality_attrs__ :
        #     print(k,getattr(self, k), getattr(other, k), getattr(self, k) == getattr(other, k) )
        return all(getattr(self, k) == getattr(other, k) for k in self.__equality_attrs__ if k)

    @property
    def description(self):
        d = getattr(self, "_description", None)
        if d is not None:
            return d
        d = (
            dbsession.query(ClanDescriptionHistory.description)
            .filter(ClanDescriptionHistory.id == self.description_id)
            .scalar()
        )
        # print("result d = ", d)
        self._description = d
        return d


class Player(DBBase):
    __tablename__ = "player"
    __table_args__ = {"extend_existing": True}
    tag = Column(String(12), primary_key=True)
    name = Column(String(32), index=True)
    history = relationship("PlayerHistory", lazy="joined")
    names = relationship("PlayerNameHistory", lazy="joined")

    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.tag = fmt_tag(kwargs["tag"])

    def exists(self):
        return dbsession.query(Player.tag).filter_by(tag=self.tag).scalar() is not None

    @staticmethod
    def _get_not_exist(tags):
        fs = dbsession.query(Player.tag).filter(Player.tag.in_(tags)).all()
        return set(tags) - set([x[0] for x in fs])

    @staticmethod
    def _get_last_histories(tags):
        return _get_last(PlayerHistory, tags)

    @staticmethod
    def _get_last_names(tags):
        return _get_last(PlayerNameHistory, tags)

    @staticmethod
    def _odict_from_dict(kwargs):
        return {"name": kwargs["name"], "tag": fmt_tag(kwargs["tag"])}

    @staticmethod
    def _get_name_changed(obs):
        if not obs:
            return None
        conditions = (and_(Player.tag == p["tag"], Player.name != p["name"]) for p in obs)
        q = Player.query.filter(or_(*conditions))
        return q.all()

    def last_history(self, force_db_load=False):
        if not force_db_load and self.history:
            return self.history[-1]
        lh = (
            PlayerHistory.query.filter(PlayerHistory.tag == self.tag)
            .order_by(PlayerHistory.last_check.desc())
            .first()
        )
        return lh

    def __last_history_attr__(self, attr, default=None, force_db_load=False):
        lh = self.last_history(force_db_load)
        return getattr(lh, attr) if lh else default

    @staticmethod
    def _get_name(tag):
        return dbsession.query(Player.name).filter(Player.tag == tag).scalar()

    @staticmethod
    def _get_names(tags):
        r = []
        for c in chunks(tags, _IN_TAG_CHUNK_SIZE_):
            myl = list(c)
            rs = dbsession.query(Player.tag, Player.name).filter(Player.tag.in_(myl)).all()
            names = {x[0]: x[1] for x in rs}
            r.extend([names.get(x, None) for x in myl])
        return r

    @property
    def clan_name(self):
        return self.__last_history_attr__("clan_name")

    @property
    def clan_tag(self):
        return self.__last_history_attr__("clan_tag")

    @property
    def donations(self):
        return self.__last_history_attr__("donations")

    @property
    def trophies(self):
        return self.__last_history_attr__("trophies")

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Player %r %r>" % (self.tag, self.name)

    def same_as_history(self, history):
        return (
            self.trophies == history.trophies
            and self.donations == history.donations
            and self.donationsReceived == history.donationsReceived
        )

    def get_attacks(self):
        return WarAttack.query.filter(WarAttack.attacker_tag == self.tag).all()

class PlayerNameHistory(DBBase):
    __tablename__ = "player_name_history"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    tag = Column(String(12), ForeignKey("player.tag"), index=True)
    name = Column(String(30))

    insert_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_check = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, camel_to_snake(attr), value)

    @staticmethod
    def get_id(tag, name):
        return (
            dbsession.query(PlayerNameHistory.id)
            .filter(and_(PlayerNameHistory.tag == tag, PlayerNameHistory.name == name))
            .scalar()
        )


class ClanNameHistory(DBBase):
    __tablename__ = "clan_name_history"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    tag = Column(String(12), ForeignKey("clan.tag"), index=True)
    name = Column(String(30))

    insert_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_check = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, camel_to_snake(attr), value)

    @staticmethod
    def get_id(tag, name):
        try:
            r = (
                dbsession.query(ClanNameHistory.id)
                .filter(and_(ClanNameHistory.tag == tag, ClanNameHistory.name == name))
                .all() ## TODO this should be scalar
            )
            return r[0] if r else None
        except:
            print("What the hell????", tag, name)
            raise


class ClanDescriptionHistory(DBBase):
    __tablename__ = "clan_description_history"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    tag = Column(String(12), ForeignKey("clan.tag"), index=True)
    description = Column(String(1024), index=True)

    insert_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_check = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, camel_to_snake(attr), value)

    @staticmethod
    def odict_from_dict(d):
        return {"tag": d["tag"], "description": d["description"]}

    @staticmethod
    def get_id(tag, description):
        return (
            dbsession.query(ClanDescriptionHistory.id)
            .filter(
                and_(
                    ClanDescriptionHistory.tag == tag,
                    ClanDescriptionHistory.description == description,
                )
            )
            .scalar()
        )


class PlayerHistory(DBBase):
    __tablename__ = "player_history"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    tag = Column(String(12), ForeignKey("player.tag"), index=True)
    exp_level = Column(Integer)
    role = Column(Integer)
    league_id = Column(Integer)
    trophies = Column(Integer)
    town_hall_level = Column(Integer)
    war_stars = Column(Integer)
    attack_wins = Column(Integer)
    defense_wins = Column(Integer)
    donations = Column(Integer)
    donations_received = Column(Integer)
    rank = Column(Integer)
    previous_rank = Column(Integer)
    clan_rank = Column(Integer)
    previous_clan_rank = Column(Integer)
    clan_tag = Column(String(12))
    entry_type = Column(Integer)

    insert_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_check = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __equality_attrs__ = set(
        [
            "tag",
            "exp_level",
            "role",
            "league_id",
            "trophies",
            "town_hall_level",
            "war_stars",
            "attack_wins",
            "defense_wins",
            "donations",
            "donations_received",
            "clan_tag",
        ]
    )

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            if attr == "townhallLevel":  # this is sometimes mispelled from api
                attr = "townHallLevel"
            setattr(self, camel_to_snake(attr), value)
        self.tag = fmt_tag(self.tag)
        try:
            self.rank = kwargs["legendStatistics"]["currentSeason"]["rank"]
        except KeyError:
            pass
        try:
            self.clan_tag = kwargs["clan"]["tag"]
            self.clan_name = kwargs["clan"]["name"]
        except KeyError:
            pass

    def __ne__(self, other):
        return not self == other

    def __eq__(self, other):
        if not isinstance(other, PlayerHistory):
            raise NotImplemented

        return all(getattr(self, k) == getattr(other, k) for k in self.__equality_attrs__ if k)

    def __str__(self):
        return f"({self.name}, {self.trophies}, {self.attack_wins}, {self.defense_wins})"


class Base(DBBase):
    __tablename__ = "base"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    alias = Column(String(64))
    urls = relationship("URL", secondary="bases_urls", backref=backref("bases", lazy="dynamic"))

    def __repr__(self):
        return "<Base %r %r>" % (self.id, self.alias)


class BasesUrls(DBBase):
    __tablename__ = "bases_urls"
    id = Column(Integer, primary_key=True)
    user_id = Column("base_id", Integer, ForeignKey("base.id"), index=True)
    url_id = Column("url_id", Integer, ForeignKey("url.id"), index=True)


class URL(DBBase):
    __tablename__ = "url"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    url = Column(String(1024))
    flag = Column(Integer)
    version = Column(String(64))

    insert_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self):
        return "<URL %r %r>" % (self.id, self.attack)
mths = ["2020-07", "2020-08", "2020-09", "2020-10", "2020-11", "2020-12"]
mths.extend([f"{year}-{month:02}" for year in range(2021,2030) for month in range(1,13)])
v = [f'("{datetime.strptime(month, "%Y-%m").date()}")' for month in mths]
insert_str = ",".join(v)
listen(
    LeagueSeason.__table__,
    "after_create",
    DDL(f""" INSERT INTO league_season (season) VALUES {insert_str} """),
)

# for k,v in vars(dbsession.bind).items():
#     print(k,v)
#     v2 = vars(v)
#     if v2:
#         for k2, v3 in v2.items():
#             print("    ", k2,v3)

# print("#####", dbsession.bind.url.drivername)
# import sys
# sys.exit(1)
