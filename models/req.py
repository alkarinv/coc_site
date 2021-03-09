import asyncio
import configparser
import json
import os
import sys
import time
from multiprocessing import Value

import requests
from aiolimiter import AsyncLimiter
from requests.exceptions import HTTPError

from models.exceptions import NotInWarException, WarLogPrivateException
from models.utils import env_istrue, fmt_tag

API_URL = "https://api.clashofclans.com/v1"

req_counter = Value('i', 0)
called_counter = Value('i', 0)
DEBUG = env_istrue("REQ_DEBUG", False)
OFFLINE = env_istrue("REQ_OFFLINE",False)
USE_FILES_FOR_REQS = env_istrue("REQ_USE_FILES",False)
SAVE_REQUESTS = env_istrue("REQ_SAVE_FILES",True)
start_time = time.time()
rate_limit = AsyncLimiter(20, 1)

_cwd = os.path.dirname(os.path.abspath(__file__))

_config = configparser.RawConfigParser()
_configfile = f"{_cwd}/../instance/data/auth_tokens.ini"
_config.read(_configfile)


def _fmt_url_(url):
    return url.replace("#", "%23")


class COCRequest():

    def __init__(self, auth_token = None):
        self.headers = {}
        self.ip = None
        if not OFFLINE and not auth_token:
            self.ip = str(requests.get("https://api.ipify.org").text)
            if DEBUG:
                print("My IP=", self.ip, flush=True)
        if not auth_token:
            auth_token = self._load_token()
        if not OFFLINE:
            self.headers = {"Accept": "application/json", "authorization": "Bearer " + auth_token}
        assert auth_token

    @staticmethod
    def _url_to_json_path(url):
        save_dir = f"{os.getenv('REQ_SAVE_DIR','.')}/requests"
        return os.path.join(save_dir, url.replace(API_URL, "").strip("/") +".json")

    def _load_token(self):
        _tokens = {k: v for k, v in _config.items("tokens")}
        _ips = {k.strip('"'): _tokens[v].strip('"') for k, v in _config.items("ips")}

        if not OFFLINE and DEBUG:
            print("My IP=", self.ip, _ips.get(self.ip, "IP Not Found"), flush=True)
            if not self.ip in _ips:
                sys.exit(1)

        WAIT_FOR_IP = os.environ.get("WAIT_FOR_IP", 0)
        if not OFFLINE and int(WAIT_FOR_IP) > 0 and self.ip not in __ips:
            import time
            print(f"'{self.ip}' Was not found. Waiting {WAIT_FOR_IP} (s) for IP")
            time.sleep(int(WAIT_FOR_IP))
            _config.read(__configfile)

            _tokens = {k: v for k, v in _config.items("tokens")}
            _ips = {k.strip('"'): _tokens[v].strip('"') for k, v in _config.items("ips")}

        try:
            auth_token = _ips[self.ip]
        except:
            auth_token = None

        return auth_token

    async def __get_raise(self, url, **kwargs):
        async with rate_limit:
            return await self.__get_raise__(url, **kwargs)


    async def __get_raise__(self, url, **kwargs):
        with called_counter.get_lock():
            called_counter.value += 1

        if DEBUG or req_counter.value % 100 == 0:
            print(f"{url}, {req_counter.value}, {called_counter.value}, {time.time() - start_time:.2f}, {(time.time() - start_time) / called_counter.value} avg")
        if USE_FILES_FOR_REQS:
            floc = COCRequest._url_to_json_path(url)
            if os.path.exists(floc):
                if DEBUG:
                    print(f"req.py opening '{floc}'")
                with open(floc) as f:
                    return json.load(f)
        if OFFLINE:
            raise FileNotFoundError(floc)

        r = requests.get(_fmt_url_(url),timeout=2, **kwargs)
        with req_counter.get_lock():
            req_counter.value += 1
        try:
            r.raise_for_status()
        except Exception as e:
            # print(e)
            # print(r)
            # print(r.json())
            e.json = r.json()
            raise e

        if SAVE_REQUESTS:
            floc = COCRequest._url_to_json_path(url)
            d = os.path.dirname(floc)
            os.makedirs(d, exist_ok=True)
            with open(floc, "w") as f:
                json.dump(r.json(), f)
        return r.json()

    def get_locations(self):
        url = f"{API_URL}/locations"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_war_leagues(self):
        url = f"{API_URL}/warleagues"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_top_players_country(self, country_id):
        url = f"{API_URL}/locations/{country_id}/rankings/players"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_top_clans_country(self, country_id):
        url = f"{API_URL}/locations/{country_id}/rankings/clans"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_league_group(self, clan_tag):
        url = f"{API_URL}/clans/{fmt_tag(clan_tag)}/currentwar/leaguegroup"
        try:
            return asyncio.run(self.__get_raise(url, headers=self.headers))
        except HTTPError as e:
            if 'reason' in e.json and e.json['reason'] == "notFound":
                raise NotInWarException(**e.json)
            raise

    def get_current_war(self, clan_tag):
        url = f"{API_URL}/clans/{fmt_tag(clan_tag)}/currentwar"
        try:
            return asyncio.run(self.__get_raise(url, headers=self.headers))
        except HTTPError as e:
            if 'reason' in e.json and e.json['reason'] == "accessDenied":
                raise WarLogPrivateException(**e.json)

    def get_league_war(self, war_tag):
        url = f"{API_URL}/clanwarleagues/wars/{war_tag}"
        res = asyncio.run(self.__get_raise(url, headers=self.headers))
        # print(res)
        if "state" in res and res["state"] == "notInWar":
            raise NotInWarException(**res)
        return res

    def get_clan(self, tag):
        url = f"{API_URL}/clans/{fmt_tag(tag)}"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_clan_members(self, tag):
        url = f"{API_URL}/clans/{fmt_tag(tag)}/members"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_player(self, tag):
        url = f"{API_URL}/players/{fmt_tag(tag)}"
        return asyncio.run(self.__get_raise(url, headers=self.headers))

    def get_war_log(self, tag):
        url = f"{API_URL}/clans/{fmt_tag(tag)}/warlog"
        try:
            return asyncio.run(self.__get_raise(url, headers=self.headers))
        except HTTPError as e:
            if 'reason' in e.json and e.json['reason'] == "accessDenied":
                raise WarLogPrivateException(**e.json)

