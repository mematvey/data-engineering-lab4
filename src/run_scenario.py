import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil
from kazoo.client import KazooClient

from common import BUCKET, ZK_HOSTS, s3_client

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"


def clean(run):
    zk = KazooClient(hosts=ZK_HOSTS)
    zk.start()
    if zk.exists(f"/hw4/{run}"):
        zk.delete(f"/hw4/{run}", recursive=True)
    zk.stop()

    s3 = s3_client()
    for prefix in (f"zk/{run}/", f"results/zk/{run}/"):
        for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
            s3.delete_object(Bucket=BUCKET, Key=obj["Key"])

    for path in LOGS.glob(f"{run}_*.log"):
        path.unlink()


def log_text(run, name):
    path = LOGS / f"{run}_{name}.log"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def event_time(text, needle):
    for line in text.splitlines():
        if needle in line:
            return float(line.split("t=")[1].split("c]")[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--fencing", type=int, required=True)
    parser.add_argument("--pause-after", type=int, default=20)
    parser.add_argument("--pause-seconds", type=float, default=40)
    args = parser.parse_args()

    clean(args.run)

    started = time.time()
    env = dict(os.environ, FENCING=str(args.fencing), ZK_T0=str(started), PYTHONIOENCODING="utf-8")

    def now():
        return time.time() - started

    def say(message):
        print(f"[t={now():7.2f} c] [сценарий] {message}", flush=True)

    def start(name):
        command = [sys.executable, str(ROOT / "src" / "coordinator.py"), "--run", args.run, "--name", name]
        return subprocess.Popen(command, env=env, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def wait_for(name, needle, limit):
        deadline = time.time() + limit
        while time.time() < deadline:
            if needle in log_text(args.run, name):
                return
            time.sleep(0.1)
        raise SystemExit(f"[t={now():7.2f} c] не дождался '{needle}' в журнале {name}")

    say(f"запуск FENCING={args.fencing}")

    proc_a = start("A")
    wait_for("A", "стал лидером", 60)
    proc_b = start("B")
    wait_for("B", "я запасной", 60)
    say(f"A (pid {proc_a.pid}) лидер, B (pid {proc_b.pid}) запасной")

    wait_for("A", f"чекпоинтов {args.pause_after} из", 300)

    leader = psutil.Process(proc_a.pid)
    tree = [leader] + leader.children(recursive=True)
    for process in tree:
        process.suspend()
    paused_at = now()
    say(f"приостановил лидера A (процессов {len(tree)}), у него чекпоинтов: {args.pause_after}")

    wait_for("B", "стал лидером", 120)
    backup_at = event_time(log_text(args.run, "B"), "стал лидером")
    say(f"запасной B стал лидером в t={backup_at:.2f} c, смена лидера заняла {backup_at - paused_at:.1f} с после паузы")

    time.sleep(max(0, args.pause_seconds - (now() - paused_at)))
    for process in tree:
        process.resume()
    say(f"возобновил лидера A после паузы в {now() - paused_at:.1f} с")

    code_a = proc_a.wait(timeout=300)
    code_b = proc_b.wait(timeout=300)
    say(f"оба координатора завершились: A код {code_a}, B код {code_b}")

    zk = KazooClient(hosts=ZK_HOSTS)
    zk.start()
    data, stat = zk.get(f"/hw4/{args.run}/pointer")
    zk.stop()
    say(f"указатель: версия {stat.version}, данные {json.dumps(json.loads(data), ensure_ascii=False)}")

    objects = s3_client().list_objects_v2(Bucket=BUCKET, Prefix=f"results/zk/{args.run}/").get("Contents", [])
    say(f"результатов в бакете: {len(objects)}: " + ", ".join(obj["Key"] for obj in objects))


if __name__ == "__main__":
    sys.exit(main())
