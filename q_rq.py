import time
from common import record


def job(i, s):
    if s:
        time.sleep(s)
    record("rq", i)
