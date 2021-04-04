import threading
import os
from datetime import datetime
from multiprocessing import Queue, Process

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


last = None
if __name__ == '__main__':
    process = Process(target=handle_db, args=(queue, ))
    process.start()

    for w in wars:
        now = datetime.utcnow()
        dif = w.end_time - now
        if not last or dif > last:
        print(dif.seconds, w, w.parent.round)
        t = threading.Timer(dif.seconds//3600, war_timer, [w.war_tag, w.league_round_id])
        t.start()


    process.join()

