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
import socket as _sock
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
import json, time, socket, subprocess, os

def cpu_pct():
    def read():
        vals = open('/proc/stat').readline().split()[1:]
        return [int(v) for v in vals]
    s1 = read(); time.sleep(0.8); s2 = read()
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

def hwmon_sensors():
    temps, fans, fan_percentages = {}, {}, {}
    base = '/sys/class/hwmon'
    try:
        for dev in sorted(os.listdir(base)):
            path = os.path.join(base, dev)
            try:
                name = open(os.path.join(path, 'name')).read().strip()
            except Exception:
                name = dev
            try:
                entries = sorted(os.listdir(path))
            except Exception:
                continue
            for fname in entries:
                fpath = os.path.join(path, fname)
                if fname.startswith('temp') and fname.endswith('_input'):
                    num = fname[4:-6]
                    try:
                        val = float(open(fpath).read()) / 1000
                        if not (5 < val < 200):
                            continue
                        try:
                            label = open(os.path.join(path, f'temp{num}_label')).read().strip()
                        except Exception:
                            label = f'temp{num}'
                        temps[f'{name}/{label}'] = round(val, 1)
                    except Exception:
                        pass
                elif fname.startswith('fan') and fname.endswith('_input'):
                    num = fname[3:-6]
                    try:
                        rpm = int(open(fpath).read())
                        if not (0 <= rpm <= 20000):
                            continue
                        try:
                            label = open(os.path.join(path, f'fan{num}_label')).read().strip()
                        except Exception:
                            label = f'fan{num}'
                        key = f'{name}/{label}'
                        fans[key] = rpm
                        pwm_path = os.path.join(path, f'pwm{num}')
                        if os.path.exists(pwm_path):
                            try:
                                pwm_raw = float(open(pwm_path).read().strip())
                                if 0 <= pwm_raw <= 255:
                                    fan_percentages[key] = round(pwm_raw / 255 * 100, 1)
                            except Exception:
                                pass
                    except Exception:
                        pass
    except Exception:
        pass
    return temps or None, fans or None, fan_percentages or None

def gpu_info():
    try:
        out = subprocess.check_output(
            ['nvidia-smi',
             '--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,fan.speed',
             '--format=csv,noheader,nounits'],
            text=True, timeout=5, stderr=subprocess.DEVNULL
        )
        gpus = []
        for line in out.strip().splitlines():
            p = [x.strip() for x in line.split(',')]
            if len(p) < 5:
                continue
            mu, mt = float(p[2]), float(p[3])
            gpu = {
                'name':         p[0],
                'util':         float(p[1]),
                'mem_used_gb':  round(mu / 1024, 2),
                'mem_total_gb': round(mt / 1024, 2),
                'mem_percent':  round(mu / mt * 100, 1) if mt else 0.0,
                'temp':         float(p[4]),
            }
            # fan.speed returns '[N/A]' on server GPUs (A100, etc.)
            if len(p) >= 6:
                try:
                    gpu['fan_pct'] = float(p[5])
                except (ValueError, TypeError):
                    pass
            gpus.append(gpu)
        return gpus or None
    except Exception:
        return None

temps, fans, fan_percentages = hwmon_sensors()
ram_pct, ram_used, ram_total = mem_info()
print(json.dumps({
    'hostname':    socket.gethostname(),
    'os':          'Linux',
    'cpu_percent': cpu_pct(),
    'cpu_cores':   cpu_cores(),
    'ram_percent': ram_pct,
    'ram_used_gb': ram_used,
    'ram_total_gb':ram_total,
    'temps':       temps,
    'fans':        fans,
    'fan_percentages': fan_percentages,
    'gpus':        gpu_info(),
}))
"""

# Build the SSH command once (base64 avoids any shell-quoting nightmare)
_ENC     = base64.b64encode(_REMOTE.strip().encode()).decode()
SSH_CMD  = f"python3 -c \"import base64; exec(base64.b64decode('{_ENC}').decode())\""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _best_cpu_temp(temps: dict | None) -> float | None:
    """Return the most representative CPU temperature from a hwmon temps dict.
    Priority: Package id (Intel) > Tdie/Tctl (AMD) > any coretemp > first value."""
    if not temps:
        return None
    for k, v in temps.items():
        if 'package' in k.lower():
            return v
    for k, v in temps.items():
        if any(x in k.lower() for x in ('tdie', 'tctl', 'tccd')):
            return v
    for k, v in temps.items():
        if 'coretemp' in k.lower() or 'core' in k.lower():
            return v
    return next(iter(temps.values()), None)


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

        transport = client.get_transport()
        if transport:
            # SSH-level keepalive: sends an ignore packet every 15 s
            transport.set_keepalive(15)

            # TCP-level keepalive — prevents cloud/NAT firewalls from
            # silently dropping the idle connection between polls
            try:
                sock = transport.sock
                sock.setsockopt(_sock.SOL_SOCKET, _sock.SO_KEEPALIVE, 1)
                # Start probing after 10 s of idle
                sock.setsockopt(_sock.IPPROTO_TCP, _sock.TCP_KEEPIDLE, 10)
                # Probe every 5 s
                sock.setsockopt(_sock.IPPROTO_TCP, _sock.TCP_KEEPINTVL, 5)
                # Declare dead after 3 failed probes (= 25 s total)
                sock.setsockopt(_sock.IPPROTO_TCP, _sock.TCP_KEEPCNT, 3)
            except (AttributeError, OSError):
                pass  # Not available on all platforms (fine on Linux/Docker)

        self._ssh = client
        self._errors = 0
        log.info("✓ Connected  %s (%s:%d)", self.sid, self.host, self.port)

    # ── Connection health check ───────────────────────────────────────────────

    def _is_alive(self) -> bool:
        """Send an SSH no-op to verify the connection is truly alive."""
        if self._ssh is None:
            return False
        transport = self._ssh.get_transport()
        if not transport or not transport.is_active():
            return False
        try:
            transport.send_ignore()
            return True
        except Exception:
            return False

    # ── Single poll cycle ─────────────────────────────────────────────────────

    def _poll_once(self):
        if not self._is_alive():
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

        # Pick the best CPU temperature from labeled hwmon sensors
        cpu_temp = _best_cpu_temp(data.get("temps"))

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
            "cpu_temp":     cpu_temp,
            "temps":        data.get("temps"),
            "fans":         data.get("fans"),
            "fan_percentages": data.get("fan_percentages"),
            "gpus":         data.get("gpus"),
        }

        resp = requests.post(f"{API_BASE}/api/metrics", json=payload, timeout=5)
        resp.raise_for_status()

        self._errors = 0
        parts = [f"CPU={data['cpu_percent']}%", f"RAM={data['ram_percent']}%"]
        if cpu_temp is not None:
            parts.append(f"temp={cpu_temp}°C")
        if data.get("fans"):
            rpms = list(data["fans"].values())
            parts.append(f"fans={','.join(str(r) for r in rpms[:2])}rpm")
        if data.get("fan_percentages"):
            fan_pcts = list(data["fan_percentages"].values())
            parts.append(f"fan%={','.join(str(round(p)) for p in fan_pcts[:2])}%")
        if data.get("gpus"):
            g = data["gpus"][0]
            parts.append(f"GPU={g['util']}%")
            if g.get("fan_pct") is not None:
                parts.append(f"gpufan={g['fan_pct']}%")
        log.info("%-20s %s", self.sid, "  ".join(parts))

    # ── Main loop (runs in a daemon thread) ───────────────────────────────────

    def run(self):
        while True:
            start = time.time()
            try:
                self._poll_once()
            except (paramiko.SSHException, OSError, EOFError,
                    ConnectionResetError, TimeoutError) as exc:
                self._errors += 1
                log.warning("%-20s SSH error #%d: %s", self.sid, self._errors, exc)
                self._ssh = None  # Force full reconnect next iteration
                # Short backoff: 5 s, 10 s, 15 s … capped at 30 s
                time.sleep(min(5 * self._errors, 30))
            except requests.RequestException as exc:
                self._errors += 1
                log.warning("%-20s API error #%d: %s", self.sid, self._errors, exc)
                time.sleep(10)
            except Exception as exc:
                self._errors += 1
                log.error("%-20s Error #%d: %s", self.sid, self._errors, exc)
                # Any unexpected error might be SSH-related — reset connection
                self._ssh = None
                time.sleep(min(5 * self._errors, 30))

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
