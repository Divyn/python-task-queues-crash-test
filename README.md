# 6 Python task queues: how fast, and what survives a crash?

Celery, Dramatiq, RQ, Huey, Taskiq and arq on the same Redis, every setting at its default, each started with its own worker command.

- Speed: queue 10,000 jobs that each add one number to a Redis set, then start the worker and time until all 10,000 are in the set. Median of 3 runs.
- Crash: queue 1,000 jobs that each sleep 50 ms first. Once 200 have finished, `kill -9` the whole worker (like a power cut), start it again and wait 420 s. Lost = jobs that never finished; doubled = extra runs of a job that already ran. 3 runs, all 6 queues at once, each on its own Redis database. Median.

| Queue | Seconds for 10,000 | Jobs/s | Lost of 1,000 | Doubled |
|---|---|---|---|---|
| RQ 3.0.0 | 255.6 | 39 | 0 | 0 |
| arq 0.25.0 | 50.1 | 200 | 0 | 1 |
| Celery 5.7.0 | 23.6 | 424 | 5 | 0 |
| Taskiq 0.13.0 | 14.1 | 711 | 14 | 0 |
| Huey 3.5.4 | 10.0 | 998 | 1 | 0 |
| Dramatiq 2.2.1 | 7.2 | 1,398 | 16 | 0 |

Run 2026-11-19 on a Vultr `vc2-1c-1gb` server (1 vCPU Intel Haswell, 961 MB RAM), Seattle, Debian 13, Python 3.13.5, Redis 8.0.2 on the same box.

- `setup.sh`: Redis and the 6 libraries.
- `common.py` and `q_*.py`: one job definition per queue. `tq_race.py`: both tests.
- `results/`: raw output, logs and the exact package versions.

## Run it yourself

1. Make a fresh Debian 13 server (these runs used Vultr `vc2-1c-1gb`: 1 vCPU, 1 GB, $5 a month).
2. Copy this folder to `/root/exp` on it, then run `cd /root/exp && sh run.sh`.
3. `run.sh` writes its state to `STATUS` (SETTING_UP, RUNNING, DONE, or SETUP_FAILED / RUN_FAILED). When it says DONE, the results are in `results.csv` and `run.log`.
