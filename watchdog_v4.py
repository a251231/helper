# -*- coding: utf-8 -*-
"""
Watchdog for WeChat 4.1 (Weixin.exe) + server_v4.py (:8080)
Monitors Weixin.exe process and Gateway V4 health without disturbing QR scan or active sessions.
"""
import json
import os
import subprocess
import time
import urllib.request
import psutil

LOG_FILE = r"C:\wcf_service\watchdog_v4.log"


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    print(line, end="")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def is_another_watchdog_running() -> bool:
    my_pid = os.getpid()
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if p.info["pid"] == my_pid:
                continue
            cmdline = " ".join(p.info.get("cmdline") or []).lower()
            if "watchdog" in cmdline and "bridge_watchdog" not in cmdline:
                if time.time() - p.create_time() < 180:
                    return True
                p.kill()
        except Exception:
            pass
    return False


def get_weixin_pid() -> int | None:
    best_pid = None
    best_mem = -1
    for p in psutil.process_iter(["pid", "name", "memory_info"]):
        try:
            if (p.info["name"] or "").lower() == "weixin.exe":
                mem = p.info["memory_info"].rss if p.info.get("memory_info") else 0
                if mem > best_mem:
                    best_mem = mem
                    best_pid = p.info["pid"]
        except Exception:
            pass
    return best_pid


def check_gateway() -> dict | None:
    for attempt in range(2):
        try:
            req = urllib.request.Request("http://127.0.0.1:8080/api/status")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except Exception:
            if attempt == 0:
                time.sleep(2)
    return None


def get_gateway_server_age() -> float | None:
    """Return age in seconds of the newest running server_v4.py process, or None."""
    my_pid = os.getpid()
    newest_age = None
    for p in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        try:
            if p.info["pid"] == my_pid:
                continue
            cmdline = " ".join(p.info.get("cmdline") or []).lower()
            if "server_v4.py" in cmdline:
                age = time.time() - (p.info.get("create_time") or 0)
                if newest_age is None or age < newest_age:
                    newest_age = age
        except Exception:
            pass
    return newest_age


def kill_gateway_servers() -> None:
    my_pid = os.getpid()
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if p.info["pid"] == my_pid:
                continue
            cmdline = " ".join(p.info.get("cmdline") or []).lower()
            if "server_v4.py" in cmdline or "server.py" in cmdline:
                log(f"Killing old gateway server (PID {p.info['pid']})...")
                p.kill()
        except Exception:
            pass


def start_server_v4_task() -> None:
    kill_gateway_servers()
    time.sleep(1)
    log("Starting server_v4.py via WeChatFerry scheduled task...")
    subprocess.run("schtasks /run /tn WeChatFerry", shell=True)
    time.sleep(4)


def start_weixin_task() -> None:
    log("Launching Weixin.exe (4.1) via StartWeixin4 scheduled task...")
    subprocess.run("schtasks /run /tn StartWeixin4", shell=True)
    time.sleep(6)


def main() -> None:
    if is_another_watchdog_running():
        return

    pid = get_weixin_pid()
    if not pid:
        log("Weixin.exe is not running! Starting Weixin.exe...")
        start_weixin_task()

    status = check_gateway()
    if status is None or status.get("engine") != "v4":
        age = get_gateway_server_age()
        if age is not None and age < 75.0:
            log(f"server_v4.py is still starting up (age={age:.1f}s < 75s), skipping restart.")
            return
        log("Gateway V4 on :8080 is not responding! Starting server_v4.py...")
        start_server_v4_task()
        return


if __name__ == "__main__":
    main()
