import asyncio
from arq.connections import RedisSettings
import redis.asyncio as aredis

_r = None


async def job(ctx, i, s):
    global _r
    if _r is None:
        _r = aredis.Redis(db=0)
    if s:
        await asyncio.sleep(s)
    await _r.sadd("done:arq", i)
    await _r.incr("runs:arq")


class WorkerSettings:
    functions = [job]
    redis_settings = RedisSettings(database=6)
