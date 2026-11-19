import time
from celery import Celery
from common import record

app = Celery("q_celery", broker="redis://localhost:6379/1")
app.conf.task_ignore_result = True


@app.task
def job(i, s):
    if s:
        time.sleep(s)
    record("celery", i)
