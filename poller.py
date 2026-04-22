#!/usr/bin/env python3
"""
SSH Poller — connects to each server via SSH, executes a stdlib-only
Python snippet in memory, and pushes the metrics to the local API.
Nothing is installed or left on the monitored servers.

Config : servers.yml  (or SERVERS_CONFIG env var)
Target : http://localhost:8080  (or MONITOR_SERVER env var)
"""

import base64
import json
import logging
import os
import sys
import time
from threading import Thread
from typing import Optional

import paramiko
import requests
import yaml

logging.basicConfig(
    format="%(asctime)s [poller] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("poller")

CONFIG_PATH = os.getenv("SERVERS_CONFIG", "/app/servers.yml")
API_BASE    = os.getenv("MONITOR_SERVER",  "http://localhost:8080")

# ── Script executed remotely (only Python stdlib, nothing to install) ────────
# Sent encoded in base64 to avoid all shell-quoting issues.
_REMOTE = r"""
import json, time, socket, subprocess

def cpu_pct():
    def read():
        vals = open('/proc/stat').readline().split()[1:]
        return [int(v) for v in vals]
    s1 = read()
    time.sleep(0.8)
    s2 = read()
    total = sum(b - a for a, b in zip(s1, s2))
    idle  = (s2[3] - s1[3]) + (s2[4] - s1[4])
    return round(100.0 * (1.0 - idle / total), 1) if total else 0.0

def mem_info():
    m = {}
    for line in open('/proc/meminfo'):
        k, v = line.split(':', 1)
        m[k.strip()] = int(v.strip().split()[0]) * 1024
    total = m['MemTotal']
    avail = m.get('MemAvailable', m.get('MemFree', 0))
    used  = total - avail
    return round(used / total * 100, 1), round(used / 1e9, 2), round(total / 1e9, 2)

def cpu_cores():
    try:
        r = open('/sys/devices/system/cpu/present').read().strip()
        return int(r.split('-')[-1]) + 1
    except Exception:
        pass
    n = 0
    for line in open('/proc/cpuinfo'):
        if line.startswith('processor'):
            n += 1
    return n or 1

def cpu_temp():
    for i in range(20):
        try:
            t = float(open(f'/sys/class/thermal/thermal_zone{i}/temp').read()) / 1000
            if 10 < t < 120:
                return round(t, 1)
        except Exception:
            pass
    return None

def gpu_info():
    try:
        out = subprocess.check_output(
            ['nvidia-smi',
             '--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu',
             '--format=csv,noheader,nounits'],
            text=True, timeout=5, stderr=subprocess.DEVNULL
        )
        gpus = []
        for line in out.strip().splitlines():
            p = [x.strip() for x in line.split(',')]
            if len(p) == 5:
                mu, mt = float(p[2]), float(p[3])
                gpus.append({
                    'name': p[0],
                    'util': float(p[1]),
                    'mem_used_gb':  round(mu / 1024, 2),
                    'mem_total_gb': round(mt / 1024, 2),
                    'mem_percent':  round(mu / mt * 100, 1) if mt else 0.0,
                    'temp': float(p[4]),
                })
        return gpus or None
    except Exception:
        return None

ram_pct, ram_used, ram_total = mem_info()
print(json.dumps({
    'hostname':    socket.gethostname(),
    'os':          'Linux',
    'cpu_percent': cpu_pct(),
    'cpu_cores':   cpu_cores(),
    'ram_percent': ram_pct,
    'ram_used_gb': ram_used,
    'ram_total_gb':ram_total,
    'cpu_temp':    cpu_temp(),
    'gpus':        gpu_info(),
}))
"""

# Build the SSH command once (base64 avoids any shell-quoting nightmare)
_ENC     = base64.b64encode(_REMOTE.strip().encode()).decode()
SSH_CMD  = f"python3 -c \"import base64; exec(base64.b64decode('{_ENC}').decode())\""


# ── Per-server poller ─────────────────────────────────────────────────────────

class ServerPoller:
    def __init__(self, cfg: dict, interval: int):
        self.sid      = cfg["id"]
        self.host     = cfg["host"]
        self.port     = int(cfg.get("port", 22))
        self.user     = cfg["user"]
        self.key_path = cfg.get("ssh_key")
        self.password = cfg.get("password")
        self.interval = interval
        self._ssh: Optional[paramiko.SSHClient] = None
        self._errors  = 0

    # ── SSH connection ────────────────────────────────────────────────────────

    def _connect(self):
        if self._ssh:
            try:
                self._ssh.close()
            except Exception:
                pass
            self._ssh = None

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        kw: dict = {
            "hostname": self.host,
            "port":     self.port,
            "username": self.user,
            "timeout":  10,
            "banner_timeout": 15,
        }
        if self.key_path:
            kw["key_filename"] = self.key_path
        elif self.password:
            kw["password"] = self.password
        else:
            # Fall back to the SSH agent / default key files
            kw["look_for_keys"] = True

        client.connect(**kw)

        # Enable keepalive to avoid idle disconnects
        transport = client.get_transport()
        if transport:
            transport.set_keepalive(30)

        self._ssh = client
        log.info("✓ Connected  %s (%s:%d)", self.sid, self.host, self.port)

    # ── Single poll cycle ─────────────────────────────────────────────────────

    def _poll_once(self):
        if self._ssh is None or not self._ssh.get_transport() or \
                not self._ssh.get_transport().is_active():
            self._connect()

        _, stdout, stderr = self._ssh.exec_command(SSH_CMD, timeout=20)
        exit_code = stdout.channel.recv_exit_status()
        output    = stdout.read().decode().strip()
        err_msg   = stderr.read().decode().strip()

        if exit_code != 0 or not output:
            raise RuntimeError(
                f"Remote script failed (exit={exit_code}): {err_msg or 'no output'}"
            )

        data = json.loads(output)

        payload = {
            "server_id":    self.sid,
            "hostname":     data["hostname"],
            "os":           data["os"],
            "timestamp":    time.time(),
            "cpu_percent":  data["cpu_percent"],
            "cpu_cores":    data["cpu_cores"],
            "ram_percent":  data["ram_percent"],
            "ram_used_gb":  data["ram_used_gb"],
            "ram_total_gb": data["ram_total_gb"],
            "cpu_temp":     data.get("cpu_temp"),
            "gpus":         data.get("gpus"),
        }

        resp = requests.post(f"{API_BASE}/api/metrics", json=payload, timeout=5)
        resp.raise_for_status()

        self._errors = 0
        parts = [f"CPU={data['cpu_percent']}%", f"RAM={data['ram_percent']}%"]
        if data.get("cpu_temp") is not None:
            parts.append(f"temp={data['cpu_temp']}°C")
        if data.get("gpus"):
            parts.append(f"GPU={data['gpus'][0]['util']}%")
        log.info("%-20s %s", self.sid, "  ".join(parts))

    # ── Main loop (runs in a daemon thread) ───────────────────────────────────

    def run(self):
        while True:
            start = time.time()
            try:
                self._poll_once()
            except (paramiko.SSHException, OSError, EOFError, ConnectionResetError) as exc:
                self._errors += 1
                log.warning("%-20s SSH error #%d: %s", self.sid, self._errors, exc)
                self._ssh = None
                time.sleep(min(5 * self._errors, 60))
            except requests.RequestException as exc:
                self._errors += 1
                log.warning("%-20s API error #%d: %s", self.sid, self._errors, exc)
                time.sleep(10)
            except Exception as exc:
                self._errors += 1
                log.error("%-20s Error #%d: %s", self.sid, self._errors, exc)
                time.sleep(5)

            elapsed = time.time() - start
            time.sleep(max(0.0, self.interval - elapsed))


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def wait_for_api(url: str, max_wait: int = 60):
    """Block until the monitoring API is reachable (handles Docker start order)."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            requests.get(f"{url}/api/servers", timeout=3)
            log.info("API reachable at %s", url)
            return
        except Exception:
            time.sleep(2)
    log.warning("API not reachable after %ds — continuing anyway", max_wait)


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        log.error("Config file not found: %s", path)
        sys.exit(1)
    with open(path) as f:
        cfg = yaml.safe_load(f)
    if not cfg or "servers" not in cfg:
        log.error("No 'servers' key in %s", path)
        sys.exit(1)
    return cfg


def main():
    cfg      = load_config(CONFIG_PATH)
    interval = int(cfg.get("poll_interval", 30))
    servers  = cfg["servers"]

    log.info("Loaded %d server(s), poll interval: %ds", len(servers), interval)
    wait_for_api(API_BASE)

    threads = []
    for srv_cfg in servers:
        poller = ServerPoller(srv_cfg, interval)
        t = Thread(target=poller.run, name=f"poll-{srv_cfg['id']}", daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5)  # Stagger initial connections

    log.info("All pollers running — Ctrl+C to stop")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down poller")


if __name__ == "__main__":
    main()
