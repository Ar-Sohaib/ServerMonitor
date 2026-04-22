#!/bin/sh
set -e

echo "[start] Server Monitor starting..."

# Start the SSH poller in the background if a config file is present
if [ -f /app/servers.yml ]; then
    echo "[start] servers.yml found — starting SSH poller"
    python /app/poller.py &
    echo "[start] Poller PID: $!"
else
    echo "[start] No servers.yml found — SSH poller disabled"
    echo "[start] To enable polling: mount servers.yml and SSH keys (see docker-compose.yml)"
fi

# Start the monitoring server in the foreground (Docker tracks this process)
echo "[start] Starting monitoring server on port ${MONITOR_PORT:-8080}"
exec python /app/server.py
