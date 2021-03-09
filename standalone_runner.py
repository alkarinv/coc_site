import sys
import time

import legends_progress as lp


def run_legends():
    while True:
        try:
            lp.check_players()
        except Exception as e:
            print(e, file=sys.stderr)

        time.sleep(130)

if __name__ == "__main__":
    run_legends()
