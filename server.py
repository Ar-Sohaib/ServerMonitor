#!/usr/bin/env python3
"""
Central monitoring server.
Receives metrics from agents, stores up to 12h, serves the dashboard,
and pushes live updates to connected browsers via WebSocket.

Usage:
    python server.py
    MONITOR_HOST=0.0.0.0 MONITOR_PORT=8080 python server.py
"""

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

HOST = os.getenv("MONITOR_HOST", "0.0.0.0")
PORT = int(os.getenv("MONITOR_PORT", "8080"))
HISTORY_SECONDS = 12 * 3600  # 12 hours rolling window

app = FastAPI(title="Server Monitor", docs_url=None, redoc_url=None)

# In-memory storage
metrics_store: dict[str, deque] = defaultdict(deque)
server_registry: dict[str, dict] = {}
ws_clients: set[WebSocket] = set()


class Metrics(BaseModel):
    server_id: str
    hostname: str
    os: str
    timestamp: float
    cpu_percent: float
    cpu_cores: int = 1
    ram_percent: float
    ram_used_gb: float
    ram_total_gb: float
    cpu_temp: Optional[float] = None
    temps: Optional[dict] = None   # all labeled temperature sensors from hwmon
    fans: Optional[dict] = None    # all fan RPMs from hwmon
    gpus: Optional[list[Any]] = None


@app.post("/api/metrics")
async def receive_metrics(m: Metrics):
    data = m.model_dump()
    sid = data["server_id"]
    now = time.time()

    store = metrics_store[sid]
    store.append(data)

    # Evict data older than 12h
    cutoff = now - HISTORY_SECONDS
    while store and store[0]["timestamp"] < cutoff:
        store.popleft()

    server_registry[sid] = {
        "last_seen": now,
        "hostname": data["hostname"],
        "os": data["os"],
    }

    # Push live update to all connected dashboard clients
    if ws_clients:
        msg = json.dumps({"type": "update", "server_id": sid, "data": data})
        dead: set[WebSocket] = set()
        for ws in list(ws_clients):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.add(ws)
        ws_clients.difference_update(dead)

    return {"ok": True}


@app.get("/api/servers")
def list_servers():
    now = time.time()
    return {
        sid: {
            **info,
            "online": (now - info["last_seen"]) < 150,
            "latest": metrics_store[sid][-1] if metrics_store[sid] else None,
        }
        for sid, info in server_registry.items()
    }


@app.get("/api/history/{server_id}")
def get_history(server_id: str):
    return list(metrics_store.get(server_id, []))


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)

    # Send the latest snapshot of every known server to the new client
    for sid in list(server_registry):
        store = metrics_store[sid]
        if store:
            try:
                await ws.send_text(json.dumps({
                    "type": "update",
                    "server_id": sid,
                    "data": store[-1],
                }))
            except Exception:
                break

    try:
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                # Keepalive ping
                await ws.send_text('{"type":"ping"}')
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        ws_clients.discard(ws)  # discard is safe even if ws was already removed


# Static files must be mounted LAST so API routes take priority
app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
