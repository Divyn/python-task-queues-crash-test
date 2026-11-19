"""6 Python task queues on one 1 vCPU, 1 GB server, all on Redis, every setting at its default.

Speed: queue 10,000 jobs that each add one number to a Redis set, then start the queue's own
worker command and time until all 10,000 are in the set. 3 runs per queue, rotating order;
the median counts. A queue that takes over 600 s is TIMEOUT.

Crash: queue 1,000 jobs that each sleep 50 ms first. Once 200 have finished, kill -9 the whole
worker (power-cut style), start it again, and wait 420 s. Lost = jobs that never finished.
Doubled = extra runs of a job that already ran. 3 crash runs, all 6 queues at once (each on its
own Redis database), median reported.
"""

import asyncio
import os
import signal
import statistics
import subprocess
import sys
import time

import redis

V = "/root/v/bin/"
WORKERS = {
    "celery":   [V + "celery", "-A", "q_celery", "worker", "--loglevel=WARNING"],
    "dramatiq": [V + "dramatiq", "q_dramatiq"],
    "rq":       [V + "rq", "worker", "--url", "redis://localhost:6379/3"],
    "huey":     [V + "huey_consumer", "q_huey.huey"],
    "taskiq":   [V + "taskiq", "worker", "q_taskiq:broker"],
    "arq":      [V + "arq", "q_arq.WorkerSettings"],
}
DBS = {"celery": 1, "dramatiq": 2, "rq": 3, "huey": 4, "taskiq": 5, "arq": 6}
N_SPEED, N_CRASH, KILL_AT, CRASH_WAIT, SPEED_LIMIT, REPEATS = 10_000, 1_000, 200, 420, 600, 3
R = redis.Redis(db=0)


def log(msg: str) -> None:
    print("# " + msg, file=sys.stderr, flush=True)


def enqueue_fresh(name: str, n: int, s: float) -> None:
    """Queue in a fresh process each time, so no client state (event loops, pools) carries over."""
    subprocess.run([V + "python", "/root/exp/tq_race.py", "enqueue", name, str(n), str(s)],
                   cwd="/root/exp", check=True)


def reset(name: str) -> None:
    redis.Redis(db=DBS[name]).flushdb()
    R.delete("done:" + name, "runs:" + name)


def enqueue(name: str, n: int, s: float) -> None:
    """Queue n jobs with each library's own call."""
    if name == "celery":
        from q_celery import job
        for i in range(n):
            job.delay(i, s)
    elif name == "dramatiq":
        from q_dramatiq import job
        for i in range(n):
            job.send(i, s)
    elif name == "rq":
        from rq import Queue
        from q_rq import job
        q = Queue(connection=redis.Redis(db=3))
        for i in range(n):
            q.enqueue(job, i, s)
    elif name == "huey":
        from q_huey import job
        for i in range(n):
            job(i, s)
    elif name == "taskiq":
        from q_taskiq import broker, job

        async def go():
            await broker.startup()
            for i in range(n):
                await job.kiq(i, s)
            await broker.shutdown()
        asyncio.run(go())
    elif name == "arq":
        from arq import create_pool
        from arq.connections import RedisSettings

        async def go():
            pool = await create_pool(RedisSettings(database=6))
            for i in range(n):
                await pool.enqueue_job("job", i, s)
            await pool.aclose()
        asyncio.run(go())


def start(name: str) -> subprocess.Popen:
    """Each worker gets its own process group, so one kill reaches every child it forked."""
    return subprocess.Popen(WORKERS[name], cwd="/root/exp", start_new_session=True,
                            stdout=open(f"/root/exp/worker_{name}.log", "a"), stderr=subprocess.STDOUT)


def kill(p: subprocess.Popen) -> None:
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    p.wait()


def done(name: str) -> int:
    return R.scard("done:" + name)


def speed(name: str) -> tuple[str, float]:
    reset(name)
    try:
        enqueue_fresh(name, N_SPEED, 0)
    except subprocess.CalledProcessError:
        return "ENQUEUE_FAILED", 0.0
    t0 = time.perf_counter()
    p = start(name)
    while done(name) < N_SPEED:
        if time.perf_counter() - t0 > SPEED_LIMIT:
            kill(p)
            return "TIMEOUT", SPEED_LIMIT
        if p.poll() is not None:
            return "WORKER_EXITED", 0.0
        time.sleep(0.05)
    secs = time.perf_counter() - t0
    kill(p)
    return "OK", secs


def crash_round() -> dict:
    """All six at once: queue, start, kill at 200, restart, wait, count."""
    for n in WORKERS:
        reset(n)
        try:
            enqueue_fresh(n, N_CRASH, 0.05)
        except subprocess.CalledProcessError:
            log(f"crash: enqueue failed for {n}")
    procs = {n: start(n) for n in WORKERS}
    killed = set()
    t0 = time.time()
    while len(killed) < len(WORKERS) and time.time() - t0 < 300:
        for n in WORKERS:
            if n not in killed and done(n) >= KILL_AT:
                kill(procs[n])
                procs[n] = start(n)
                killed.add(n)
                log(f"crash: killed and restarted {n}")
        time.sleep(0.02)
    t1 = time.time()
    while time.time() - t1 < CRASH_WAIT and any(done(n) < N_CRASH for n in WORKERS):
        time.sleep(1)
    out = {}
    for n in WORKERS:
        kill(procs[n])
        uniq, runs = done(n), int(R.get("runs:" + n) or 0)
        out[n] = (N_CRASH - uniq, runs - uniq, "KILLED" if n in killed else "NEVER_REACHED_200")
    return out


def main() -> None:
    names = list(WORKERS)
    print("# " + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    sp = {n: [] for n in names}
    st = {n: [] for n in names}
    for rep in range(REPEATS):
        for n in names[rep:] + names[:rep]:
            s, secs = speed(n)
            log(f"speed {rep + 1} {n} {s} {secs:.2f}s")
            st[n].append(s)
            if s == "OK":
                sp[n].append(secs)
    crash = {n: [] for n in names}
    for rep in range(REPEATS):
        for n, v in crash_round().items():
            crash[n].append(v)
            log(f"crash {rep + 1} {n} lost={v[0]} doubled={v[1]} {v[2]}")
    print("queue,median_s_10k,jobs_per_s,speed_runs,speed_status,median_lost_of_1000,median_doubled,crash_runs")
    for n in names:
        med = statistics.median(sp[n]) if sp[n] else None
        lost = statistics.median(c[0] for c in crash[n])
        dup = statistics.median(c[1] for c in crash[n])
        runs = " ".join(f"{c[0]}/{c[1]}/{c[2]}" for c in crash[n])
        print(f"{n},{'' if med is None else f'{med:.2f}'},{'' if med is None else f'{N_SPEED / med:.0f}'},"
              f"{' '.join(f'{x:.2f}' for x in sp[n])},{' '.join(st[n])},{lost},{dup},{runs}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "enqueue":
        enqueue(sys.argv[2], int(sys.argv[3]), float(sys.argv[4]))
    else:
        main()
