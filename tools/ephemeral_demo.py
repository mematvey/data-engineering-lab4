import re
import subprocess
import threading
import time
from pathlib import Path

from zkcmd import PORTS, clean, run

PROMPT = f"PS {Path(__file__).resolve().parent.parent}> "


def show(server, command, window):
    print(f"{PROMPT}docker exec {server}-hw4 zkCli.sh -server {server}:{PORTS[server]} {command}     # окно {window}")
    for line in run(server, command):
        print(line)


def answer(lines, start):
    text = re.sub(r"\[zk: [^\]]*\] ", "", "".join(lines[start:]))
    text = re.sub(r"\d{2}:\d{2}:\d{2}\.\d+ \[main\] DEBUG [^\n]*\n", "", text)
    return clean(text)


def main():
    print(f"{PROMPT}docker exec -i zk2-hw4 zkCli.sh -server zk2:2182     # окно 2")
    window2 = subprocess.Popen(
        ["docker", "exec", "-i", "zk2-hw4", "zkCli.sh", "-server", "zk2:2182"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    lines = []
    threading.Thread(target=lambda: lines.extend(window2.stdout), daemon=True).start()
    time.sleep(4)

    mark = len(lines)
    for number, command in enumerate(("create -e /demo-ephemeral mematvey", "stat /demo-ephemeral")):
        window2.stdin.write(command + "\n")
        window2.stdin.flush()
        time.sleep(1.5)
        print(f"[zk: zk2:2182(CONNECTED) {number}] {command}")
        for line in answer(lines, mark):
            print(line)
        mark = len(lines)

    print()
    show("zk1", "ls /", 1)
    show("zk1", "get /demo-ephemeral", 1)

    print("\n[zk: zk2:2182(CONNECTED) 2] quit     # закрываю окно 2")
    window2.stdin.write("quit\n")
    window2.stdin.flush()
    window2.wait(timeout=20)
    for line in answer(lines, mark):
        print(line)
    time.sleep(1)

    print()
    show("zk1", "ls /", 1)
    show("zk1", "get /demo-ephemeral", 1)


if __name__ == "__main__":
    main()
