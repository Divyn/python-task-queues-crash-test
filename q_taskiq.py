import asyncio
import redis.asyncio as aredis
from taskiq_redis import ListQueueBroker

broker = ListQueueBroker("redis://localhost:6379/5")
_r = None


@broker.task
async def job(i, s):
    global _r
    if _r is None:
        _r = aredis.Redis(db=0)
    if s:
        await asyncio.sleep(s)
    await _r.sadd("done:taskiq", i)
    await _r.incr("runs:taskiq")
