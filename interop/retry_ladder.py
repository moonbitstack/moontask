"""The same retry ladder on Celery that src/examples/03-retry runs here.

One task, two retries, a wait that doubles: 0.5 s before the second attempt and
1 s before the third. Each attempt writes the time it started, so the transcript
can be compared with the MoonBit one line for line.

    python retry_ladder.py            # prints one JSON object per line
"""

import json
import subprocess
import sys
import time

from celery import Celery
from redis import Redis

BROKER = "redis://127.0.0.1:6379/0"

app = Celery("ladder", broker=BROKER, backend=BROKER)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_default_queue="ladder",
    worker_prefetch_multiplier=1,
)


@app.task(bind=True, name="t.flaky", max_retries=2)
def flaky(self):
    Redis.from_url(BROKER).rpush(
        "log:celery",
        json.dumps({"attempt": self.request.retries + 1, "at": time.time() * 1000}),
    )
    if self.request.retries < 2:
        raise self.retry(countdown=0.5 * (2 ** self.request.retries), exc=ValueError("boom"))
    return None


def main():
    redis = Redis.from_url(BROKER)
    redis.delete("log:celery")
    worker = subprocess.Popen(
        [sys.executable, "-m", "celery", "-A", "retry_ladder", "worker",
         "-Q", "ladder", "-c", "1", "-l", "warning"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(9)
        result = flaky.apply_async(queue="ladder")
        deadline = time.time() + 25
        while time.time() < deadline and not result.ready():
            time.sleep(0.05)
        runs = redis.lrange("log:celery", 0, -1)
        print(json.dumps({"side": "celery", "state": result.state, "attempts": len(runs)}))
        for raw in runs:
            entry = json.loads(raw)
            print(json.dumps({"side": "celery", "attempt": entry["attempt"], "at": round(entry["at"])}))
    finally:
        worker.terminate()
        try:
            worker.wait(5)
        except Exception:
            worker.kill()


if __name__ == "__main__":
    main()
