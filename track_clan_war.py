import argparse
import os
import threading
from datetime import datetime, timedelta
from threading import Event, Thread

from models.exceptions import WarLogPrivateException

if os.path.exists(".env"):
    import dotenv

    dotenv.load_dotenv(".env", override=True)

from models.model_controler import ModelControler
from models.req import COCRequest

mc = ModelControler()
stopFlag = Event()

class MyClanChecker(Thread):
    def __init__(self, event, mc, tag):
        Thread.__init__(self)
        self.stopped = event
        self.mc = mc
        self.tag = tag


    def run(self):
        wait_time = 1
        while not self.stopped.wait(wait_time):
            try:
                war = mc.get_current_war(self.tag, True)
                dif = war.end_time - datetime.utcnow()
                wait_time = min(dif.seconds + 5, 300)
                print(datetime.utcnow(), war)
            except WarLogPrivateException as e:
                print(datetime.utcnow(), f"WarLogPrivate {self.tag}")
                wait_time = 180


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run League.")
    ap.add_argument("--clan-tag", default=None)

    args = ap.parse_args()

    thread = MyClanChecker(stopFlag, mc)
    thread.start()
# this will stop the timer
# stopFlag.set()