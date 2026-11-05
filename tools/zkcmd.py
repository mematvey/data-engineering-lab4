import re
import subprocess
import sys

PORTS = {"zk1": 2181, "zk2": 2182, "zk3": 2183}
NOISE = re.compile(
    r"^\d{2}:\d{2}:\d{2}|^WATCHER::|^WatchedEvent state:SyncConnected|^Connecting to|^Welcome to|^JLine"
    r"|^$|^ctime =|^mtime =|^org\.apache|^at "
)


def clean(text):
    return [line for line in text.splitlines() if not NOISE.search(line.strip())]


def run(server, command, wait=20):
    args = ["docker", "exec", f"{server}-hw4", "zkCli.sh", "-server", f"{server}:{PORTS[server]}"] + command.split()
    try:
        done = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", timeout=wait, check=False
        )
        return clean(done.stdout)
    except subprocess.TimeoutExpired as error:
        out = error.stdout or ""
        if isinstance(out, bytes):
            out = out.decode("utf-8", "replace")
        return clean(out) + [f"нет ответа за {wait} с"]


if __name__ == "__main__":
    wait = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    for line in run(sys.argv[1], sys.argv[2], wait):
        print(line)
