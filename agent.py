#!/usr/bin/env python3
"""
Lightweight monitoring agent.
Collects CPU, RAM, temperature and GPU metrics, then pushes them
to the central monitoring server.  Run one instance per server.

Configuration via environment variables:
    MONITOR_SERVER   URL of the central server  (default: http://localhost:8080)
    SERVER_ID        Identifier for this server  (default: hostname)
    COLLECT_INTERVAL Seconds between collections (default: 30)

Examples:
    python agent.py
    MONITOR_SERVER=http://192.168.1.10:8080 SERVER_ID=db-01 python agent.py
"""

import os
import platform
import socket
import subprocess
import sys
import time

import psutil
import requests

SERVER_URL = os.getenv("MONITOR_SERVER", "http://localhost:8080")
SERVER_ID = os.getenv("SERVER_ID", socket.gethostname())
INTERVAL = int(os.getenv("COLLECT_INTERVAL", "30"))


def get_gpu_metrics() -> list | None:
    """Query NVIDIA GPU stats via nvidia-smi (returns None if unavailable)."""
    try:
        out = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu",
                "--format=csv,noheader,nounits",
            ],
            timeout=5,
            text=True,
            stderr=subprocess.DEVNULL,
        )
        gpus = []
        for line in out.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) == 5:
                mem_used = float(parts[2])
                mem_total = float(parts[3])
                gpus.append(
                    {
                        "name": parts[0],
                        "util": float(parts[1]),
                        "mem_used_gb": round(mem_used / 1024, 2),
                        "mem_total_gb": round(mem_total / 1024, 2),
                        "mem_percent": round(mem_used / mem_total * 100, 1) if mem_total else 0,
                        "temp": float(parts[4]),
                    }
                )
        return gpus or None
    except Exception:
        return None


def get_cpu_temperature() -> float | None:
    """Return average CPU temperature in °C (None if unavailable)."""
    try:
        temps = psutil.sensors_temperatures()
        if not temps:
            return None
        # Prefer well-known sensor names
        for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz", "it8", "nct6775"):
            if key in temps:
                vals = [t.current for t in temps[key] if t.current and t.current > 0]
                if vals:
                    return round(sum(vals) / len(vals), 1)
        # Fallback: first available sensor
        for readings in temps.values():
            vals = [t.current for t in readings if t.current and t.current > 0]
            if vals:
                return round(sum(vals) / len(vals), 1)
    except Exception:
        pass
    return None


def collect() -> dict:
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()

    payload = {
        "server_id": SERVER_ID,
        "hostname": socket.gethostname(),
        "os": platform.system(),
        "timestamp": time.time(),
        "cpu_percent": round(cpu, 1),
        "cpu_cores": psutil.cpu_count(logical=False) or psutil.cpu_count() or 1,
        "ram_percent": round(mem.percent, 1),
        "ram_used_gb": round(mem.used / 1024**3, 2),
        "ram_total_gb": round(mem.total / 1024**3, 2),
        "cpu_temp": get_cpu_temperature(),
        "gpus": get_gpu_metrics(),
    }
    return payload


def main():
    print(f"[agent] server={SERVER_URL}  id={SERVER_ID}  interval={INTERVAL}s")
    consecutive_errors = 0

    while True:
        loop_start = time.time()
        try:
            data = collect()
            resp = requests.post(f"{SERVER_URL}/api/metrics", json=data, timeout=5)
            resp.raise_for_status()
            consecutive_errors = 0
            parts = [f"CPU={data['cpu_percent']}%", f"RAM={data['ram_percent']}%"]
            if data["cpu_temp"] is not None:
                parts.append(f"temp={data['cpu_temp']}°C")
            if data["gpus"]:
                parts.append(f"GPU={data['gpus'][0]['util']}%")
            print(f"[{time.strftime('%H:%M:%S')}] " + "  ".join(parts))
        except Exception as exc:
            consecutive_errors += 1
            print(f"[{time.strftime('%H:%M:%S')}] ERROR ({consecutive_errors}): {exc}", file=sys.stderr)
            if consecutive_errors >= 5:
                # Slow down retries after repeated failures
                time.sleep(min(consecutive_errors * 10, 120))

        elapsed = time.time() - loop_start
        time.sleep(max(0, INTERVAL - elapsed))


if __name__ == "__main__":
    main()
