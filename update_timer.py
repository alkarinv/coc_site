import os
import threading
from datetime import datetime, timedelta
from multiprocessing import Process, Queue

if os.path.exists(".env"):
    import dotenv

    dotenv.load_dotenv(".env", override=True)

from models.model_controler import ModelControler
from models.req import COCRequest

queue = Queue()
DONE = "__DONE__"

db_lock = threading.Lock()


mc = ModelControler()

wars = mc.get_unfinished_wars()

def handle_db(queue):
    req = COCRequest()
    while True:
        msg = queue.get()
        print("msg=", msg)
        tag, rnd = msg
        if tag == DONE:
            break
        mc = ModelControler(req)
        war = mc.get_war_from_war_tag(tag, rnd)
        print("Found", war)

def war_timer(tag, rnd):
    print(tag, rnd)
    queue.put((tag, rnd))

def _stop_queue_():
    queue.put((DONE,-1))

class LastTime():
    def __init__(self, started_at, timer_end, remaining):
        self.started_at = started_at
        self.timer_end = timer_end
        self.remaining = remaining
        self.timer = threading.Timer(remaining, _stop_queue_)
        self.timer.start()

    def new_end(self, seconds):
        if self.timer:
            self.timer.cancel()

        self.timer = threading.Timer(seconds, _stop_queue_)
        self.timer.start()

    def ends_before(self, end_time):
        return self.timer_end < end_time


class StubWar():
    def __init__(self, et):
        self.end_time = et
        self.war_tag = str(et)
        self.league_round_id = 0

if __name__ == '__main__':
    last = None
    timers = []
    process = Process(target=handle_db, args=(queue, ))
    process.start()
    wars = []

    for i in range(10):
        now = datetime.utcnow()

        wars.append(StubWar(now + timedelta(seconds=i+10)))

    for w in wars:
        print(w)
        now = datetime.utcnow()
        dif = w.end_time - now
        if not last:
            last = LastTime(now, w.end_time, dif.seconds + 5)
        if last and last.ends_before(w.end_time):
            last.new_end(dif.seconds+5)

        # print(dif.seconds, w, w.parent.round)
        t = threading.Timer(dif.seconds, war_timer, [w.war_tag, w.league_round_id])
        t.start()
        timers.append(t)

    for t in timers:
        t.join()
    process.join()

