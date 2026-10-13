"""GeckoTerminal public DEX API client.

Keyless REST access, documented limit: 30 calls/min.
Docs: https://www.geckoterminal.com/dex-api
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

BASE_URL = "https://api.geckoterminal.com/api/v2"
NETWORK = "solana"
MIN_CALL_INTERVAL = 2.2
MAX_RETRIES = 3

_last_call = 0.0
_lock = asyncio.Lock()


async def _paced_get(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Rate-limited GET with retry on transient errors and 429."""
    global _last_call
    for _attempt in range(MAX_RETRIES):
        async with _lock:
            wait = _last_call + MIN_CALL_INTERVAL - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            _last_call = time.monotonic()
        try:
            response = await client.get(BASE_URL + path, params=params)
        except httpx.HTTPError:
            await asyncio.sleep(2.0)
            continue
        if response.status_code == 429:
            await asyncio.sleep(30.0)
            continue
        if response.status_code >= 400:
            return None
        try:
            return response.json()
        except ValueError:
            return None
    return None


async def top_pool_address(
    client: httpx.AsyncClient, token_address: str
) -> str | None:
    """Return the most liquid pool address for a token, or None."""
    data = await _paced_get(
        client,
        "/networks/" + NETWORK + "/tokens/" + token_address + "/pools",
        {"page": "1"},
    )
    pools = (data or {}).get("data") or []
    for pool in pools:
        attributes = pool.get("attributes") or {}
        address = attributes.get("address")
        if address:
            return str(address)
    return None


async def fetch_ohlcv(
    client: httpx.AsyncClient,
    pool_address: str,
    timeframe: str = "60",
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch USD OHLCV candles for a pool.

    timeframe is in minutes: 1, 5, 15, 30, 60, 240, 1440.
    """
    data = await _paced_get(
        client,
        "/networks/" + NETWORK + "/pools/" + pool_address + "/ohlcv/" + timeframe,
        {"aggregate": "true", "limit": str(limit), "currency": "usd"},
    )
    attributes = ((data or {}).get("data") or {}).get("attributes") or {}
    rows = attributes.get("ohlc") or []
    candles: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            candles.append(
                {
                    "t": int(float(row[0])),
                    "o": float(row[1]),
                    "h": float(row[2]),
                    "l": float(row[3]),
                    "c": float(row[4]),
                    "v": float(row[5]),
                }
            )
        except (TypeError, ValueError):
            continue
    candles.sort(key=lambda c: c["t"])
    return candles
