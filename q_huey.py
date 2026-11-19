import time
from huey import RedisHuey
from common import record

huey = RedisHuey("q_huey", db=4)


@huey.task()
def job(i, s):
    if s:
        time.sleep(s)
    record("huey", i)
