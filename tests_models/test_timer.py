import time
import unittest
from datetime import datetime, timedelta

from models.timer import DateTimeTimer


class AllTests(unittest.TestCase):

    def test_timer(self):
        print("starting", time.ctime())
        now = datetime.now()
        dt = now + timedelta(seconds=1)
        DateTimeTimer(dt)
        dt = now + timedelta(seconds=2)
        DateTimeTimer(dt)
        dt = now + timedelta(seconds=3)
        DateTimeTimer(dt)
        print("here", time.ctime())
        time.sleep(5)
        print("done", time.ctime())


if __name__ == "__main__":
    # unittest.main()
    timeit.timeit(AllTests().test_timer)
