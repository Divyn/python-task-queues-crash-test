"""Shared by every queue's task: record that job i ran. db 0 holds the tallies."""
import redis

R = redis.Redis(db=0)


def record(name: str, i: int) -> None:
    R.sadd("done:" + name, i)   # unique jobs that finished
    R.incr("runs:" + name)      # every execution, so a job run twice shows up
