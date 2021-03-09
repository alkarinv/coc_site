import random
import signal
import time
from datetime import datetime
from functools import wraps
from threading import Timer as tTimer


def handler(signum, frame):
    print(f"timer expired {signum} {frame}", time.ctime())


def handler2():
    print(f"timer expired", time.ctime())


class CountdownTimer:
    def __init__(self, seconds, sigma=None, minimum=None, func_handler=None):
        self.seconds = seconds
        if sigma is not None:
            self.seconds = random.gauss(seconds, sigma)
            if minimum is not None:
                self.seconds = max(self.seconds, minimum)
        self.starttime = time.time()
        if func_handler:
            t = tTimer(self.seconds, func_handler)
            t.start()

    def timeleft(self):
        return self.seconds - (time.time() - self.starttime)

    def ellapsed(self):
        return time.time() - self.starttime > self.seconds

    def reset(self, seconds=None):
        if seconds is not None:
            self.seconds = seconds
        self.starttime = time.time()

    def __str__(self):
        return "[CountdownTimer remaining=%d]" % (self.seconds - (time.time() - self.starttime))


class DateTimeTimer(CountdownTimer):
    id = 0

    def h(self):
        # print(f"{self.id} {self.seconds}   ahh yeah {signum}")
        print(f"  Done {self.id}")

    def __init__(self, dt):
        self.id = DateTimeTimer.id
        DateTimeTimer.id += 1
        now = datetime.now()
        delta = dt - now
        secs = delta.total_seconds()
        super().__init__(secs, func_handler=self.h)


class Timer:
    mult = None

    def __init__(self):
        self.time = 0
        self.start()  ## init time
        self.starttime = self.time
        self.mult = None
        self.store = False
        self.storevalue = 0

    def reset(self):
        self.start()
        self.starttime = self.time

    def millis(self):
        return int(round(time.time() * 1000))

    def start(self):
        self.time = self.millis()

    def ellapsed_print(self, header="", footer=""):
        dif = self.ellapsed()
        print(f"{header}  {dif} (ms) {footer}")
        return dif

    def ellapsed(self):
        curtime = self.millis()
        dif = curtime - self.time
        self.time = curtime
        return dif

    def greater(self, milli):
        dif = self.millis() - self.time
        return dif > milli

    def totalellapsed(self):
        return self.millis() - self.starttime

    def waitseconds(self, seconds):
        self.wait(seconds * 1000)

    def wait(self, milliseconds):
        timedif = self.time + milliseconds - self.millis()
        if self.store:
            otimedif = timedif
            # print("      ", timedif, otimedif, milliseconds, self.storevalue, flush=True)
            if self.storevalue < 0:
                timedif += self.storevalue
                self.storevalue = 0
            if timedif > 0:
                # print("waiting ", timedif, otimedif, milliseconds, self.storevalue, flush=True)
                time.sleep(timedif * 0.001)
            if otimedif < 0:
                self.storevalue = otimedif
        elif timedif > 0:
            time.sleep(timedif * 0.001)

        self.time = self.millis()

    def rawwait(self, milli):
        time.sleep(milli / 1000)

    @staticmethod
    def setMult(mult):
        Timer.mult = mult

    @staticmethod
    def sleep(seconds):
        if Timer.mult:
            seconds *= Timer.mult
        time.sleep(seconds)

    @staticmethod
    def sleepm(millis):
        if Timer.mult:
            millis *= Timer.mult
        time.sleep(millis)



def timeit(func=None, *, verbose=False):

    def _decorate(function):
        @wraps(func)
        def wrapper(*args, **kw):

            name = function.__qualname__
            if name not in timeit.ttimes:
                timeit.ttimes[name] = (0, 0)
            ts = time.time()
            result = function(*args, **kw)
            te = time.time()
            t = te - ts
            tup = timeit.ttimes[name]
            ntup = (tup[0] + 1, tup[1] + t)
            timeit.ttimes[name] = ntup
            if verbose:
                print(
                    "func<%r> took: %2.4f sec count:%d avg:%2.4f total:%2.4f"
                    % (name, t, ntup[0], ntup[1] / ntup[0], ntup[1])
                )
            return result
        return wrapper
    if timeit.counter == 0:
        import atexit
        def printall():
            import math
            for name,ntup in sorted(timeit.ttimes.items(), key=lambda x: x[1][1]):
                avg = math.nan if not ntup[0] else ntup[1]/ntup[0]
                print(
                    "func<%r> count:%d avg:%2.4f total:%2.4f"
                    % (name, ntup[0], avg, ntup[1])
                )

        atexit.register(printall)
    timeit.counter +=1
    if func:
        return _decorate(func)
    return _decorate
timeit.counter = 0
timeit.ttimes = {}