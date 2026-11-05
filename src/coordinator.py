import argparse
import hashlib
import json
import os
import sys
import threading
import time
from collections import Counter
from functools import lru_cache
from multiprocessing import Pool
from pathlib import Path

from botocore.exceptions import ClientError
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError

import mr
from common import BUCKET, ZK_HOSTS, s3_client

WORKERS = 4
SESSION_TIMEOUT = 10
LOGS = Path(__file__).resolve().parent.parent / "logs"

client = lru_cache(maxsize=1)(s3_client)


def make_log(run, name, started):
    LOGS.mkdir(exist_ok=True)
    file = open(LOGS / f"{run}_{name}.log", "a", encoding="utf-8", buffering=1)

    def log(message):
        line = f"[t={time.time() - started:7.2f} c] [{name}] {message}"
        file.write(line + "\n")
        print(line, flush=True)

    return log


def checkpoint_key(run, chunk_id):
    return f"zk/{run}/ckpt/chunk_{chunk_id:03d}.json"


def has_checkpoint(run, chunk_id):
    try:
        client().head_object(Bucket=BUCKET, Key=checkpoint_key(run, chunk_id))
        return True
    except ClientError:
        return False


def save_checkpoint(run, chunk_id, stat, pairs):
    body = {
        "chunk": chunk_id,
        "stat": dict(stat),
        "pairs": [[list(key), count] for key, count in pairs.items()],
    }
    client().put_object(
        Bucket=BUCKET,
        Key=checkpoint_key(run, chunk_id),
        Body=json.dumps(body).encode("utf-8"),
    )


def load_checkpoint(run, chunk_id):
    body = (
        client()
        .get_object(Bucket=BUCKET, Key=checkpoint_key(run, chunk_id))["Body"]
        .read()
    )
    data = json.loads(body)
    return Counter(data["stat"]), Counter(
        {tuple(key): count for key, count in data["pairs"]}
    )


def worker(task):
    run, chunk_id, start, end, size, columns = task

    if has_checkpoint(run, chunk_id):
        return chunk_id, False

    stat, pairs = mr.map_chunk(client(), columns, size, start, end)
    save_checkpoint(run, chunk_id, stat, pairs)

    return chunk_id, True


def wait_for_leadership(zk, election, name, log):
    node = zk.create(f"{election}/c-", name.encode(), ephemeral=True, sequence=True)
    mine = node.rsplit("/", 1)[1]
    log(f"встал в очередь: {mine}")

    while True:
        children = sorted(zk.get_children(election))
        if children[0] == mine:
            return

        previous = children[children.index(mine) - 1]
        log(f"я запасной, жду исчезновения узла {previous}")

        gone = threading.Event()
        if (
            zk.exists(f"{election}/{previous}", watch=lambda event: gone.set())
            is not None
        ):
            gone.wait()


def take_epoch(zk, epoch_path):
    while True:
        data, stat = zk.get(epoch_path)
        epoch = int(data) + 1

        try:
            zk.set(epoch_path, str(epoch).encode(), version=stat.version)
            return epoch
        except BadVersionError:
            continue


def calculate(run, epoch, log):
    size = mr.object_size(client())
    columns = mr.read_header(client())
    chunks = mr.split_chunks(size)
    total = len(chunks)

    todo = [
        (run, chunk_id, start, end, size, columns)
        for chunk_id, start, end in chunks
        if not has_checkpoint(run, chunk_id)
    ]
    done = total - len(todo)

    if done:
        log(f"нашёл чекпоинты: {done} из {total}, продолжаю с остатка")
    else:
        log(f"чекпоинтов нет, считаю с начала: чанков {total}, мапперов {WORKERS}")

    skipped = 0
    with Pool(WORKERS) as pool:
        for chunk_id, fresh in pool.imap_unordered(worker, todo):
            if fresh:
                done += 1
                log(
                    f"эпоха {epoch}: чанк {chunk_id} готов, чекпоинтов {done} из {total}"
                )
            else:
                skipped += 1

    if skipped:
        log(
            f"эпоха {epoch}: чекпоинты {skipped} чанков уже лежат в бакете, пересчитывать не стал"
        )

    parts = [load_checkpoint(run, chunk_id) for chunk_id, _, _ in chunks]
    stat, pairs = mr.reduce_pairs(parts)

    data = mr.to_csv_bytes(pairs)
    md5 = hashlib.md5(data).hexdigest()
    log(
        f"эпоха {epoch}: вакансий с навыками {stat['with_skills']}, пар {len(pairs)}, вхождений {sum(pairs.values())}, md5 {md5}"
    )

    return data, md5


def publish(zk, log, run, name, epoch, token, fencing, data, md5):
    key = f"results/zk/{run}/epoch_{epoch}/stack_2.csv"
    pointer = f"/hw4/{run}/pointer"
    record = json.dumps(
        {"epoch": epoch, "leader": name, "key": key, "md5": md5}
    ).encode()

    client().put_object(Bucket=BUCKET, Key=key, Body=data)

    try:
        stat = zk.set(pointer, record, version=token if fencing else -1)
    except BadVersionError:
        client().delete_object(Bucket=BUCKET, Key=key)
        _, current = zk.get(pointer)
        log(
            f"эпоха {epoch}: запись указателя отклонена, BadVersionError: ждал версию {token}, в ZooKeeper версия {current.version}"
        )
        log(f"эпоха {epoch}: свой результат {key} удалил")
        return 3

    log(
        f"эпоха {epoch}: записал результат {key}, указатель теперь версии {stat.version}"
    )
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    fencing = os.environ.get("FENCING", "0") == "1"
    started = float(os.environ.get("ZK_T0", time.time()))
    log = make_log(args.run, args.name, started)

    base = f"/hw4/{args.run}"
    election = f"{base}/election"
    epoch_path = f"{base}/epoch"
    pointer = f"{base}/pointer"

    zk = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
    zk.add_listener(lambda state: log(f"состояние сессии: {state}"))
    zk.start()
    log(
        f"подключился к ZooKeeper, таймаут сессии {SESSION_TIMEOUT} с, FENCING={int(fencing)}, сессия {zk.client_id[0]:#x}"
    )

    zk.ensure_path(election)
    for path, value in ((epoch_path, b"0"), (pointer, b"{}")):
        try:
            zk.create(path, value)
        except NodeExistsError:
            pass

    wait_for_leadership(zk, election, args.name, log)
    epoch = take_epoch(zk, epoch_path)
    token = zk.get(pointer)[1].version
    log(f"стал лидером: эпоха {epoch}, версия указателя при выборе {token}")

    data, md5 = calculate(args.run, epoch, log)
    code = publish(zk, log, args.run, args.name, epoch, token, fencing, data, md5)

    zk.stop()
    zk.close()

    return code


if __name__ == "__main__":
    sys.exit(main())
