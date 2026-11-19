import time
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from common import record

dramatiq.set_broker(RedisBroker(url="redis://localhost:6379/2"))


@dramatiq.actor
def job(i, s):
    if s:
        time.sleep(s)
    record("dramatiq", i)
