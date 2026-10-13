"""PumpPortal realtime client (free tier).

Subscribes to the two free PumpPortal streams:
  - subscribeNewToken : every new pump.fun bonding-curve launch
  - subscribeMigration: graduation (bonding curve -> PumpSwap/Raydium)

Metered streams (subscribeTokenTrade / subscribeAccountTrade,
0.01 SOL per 10k events) are intentionally NOT used.

Docs: https://pumpportal.fun/data-api/real-time/
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable

import websockets

WS_URL = "wss://pumpportal.fun/api/data"
CONNECT_TIMEOUT = 15.0
RECV_TIMEOUT = 30.0
RECONNECT_DELAY = 2.0


def classify_event(message: dict[str, Any]) -> str:
    """Best-effort classification; PumpPortal does not tag events explicitly."""
    if "solAmount" in message or "tokenAmount" in message:
        return "trade"
    if "bondingCurveKey" in message:
        return "new_token"
    return "migration"


async def listen(
    duration_s: float,
    on_event: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    """Listen to the free PumpPortal streams for at most duration_s seconds.

    Reconnects on transient errors until the time budget is exhausted and
    always returns a per-type event summary.
    """
    deadline = time.monotonic() + max(1.0, duration_s)
    summary: dict[str, Any] = {
        "received": 0,
        "new_token": 0,
        "migration": 0,
        "trade": 0,
        "unknown": 0,
        "reconnects": 0,
        "errors": 0,
    }

    while time.monotonic() < deadline:
        try:
            async with websockets.connect(
                WS_URL,
                open_timeout=CONNECT_TIMEOUT,
                ping_interval=20.0,
                ping_timeout=20.0,
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeMigration"}))
                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=min(remaining, RECV_TIMEOUT)
                        )
                    except asyncio.TimeoutError:
                        continue
                    if not isinstance(raw, str):
                        continue
                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        summary["errors"] += 1
                        continue
                    if not isinstance(message, dict):
                        continue
                    event_type = classify_event(message)
                    message["event_type"] = event_type
                    summary[event_type] = summary.get(event_type, 0) + 1
                    summary["received"] += 1
                    try:
                        on_event(message)
                    except Exception:
                        summary["errors"] += 1
                return summary
        except Exception:
            summary["reconnects"] += 1
            await asyncio.sleep(RECONNECT_DELAY)
    return summary
