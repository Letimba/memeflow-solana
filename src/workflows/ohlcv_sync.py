"""Workflow: ohlcv-sync.

Builds OHLCV history for graduated tokens via the keyless GeckoTerminal
API and stores it in the data lake. Paced well below the 30 calls/min
free limit. Only useful after pumpfun-ingest recorded graduations.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx
from pydantic import BaseModel, Field

import mistralai.workflows as workflows
from mistralai.workflows import Depends
from mistralai.workflows.plugins.mistralai.connectors import (
    ToolCallClient,
    uses_connectors,
)

from src.lib.geckoterminal import fetch_ohlcv, top_pool_address
from src.lib.storage import GitHubStore, github_connector


class OhlcvSyncParams(BaseModel):
    repo_owner: str
    repo_name: str
    branch: str = "main"
    timeframe: str = "60"
    max_tokens: int = Field(default=12, ge=1, le=50)
    candle_limit: int = Field(default=1000, ge=100, le=1000)
    batch_size: int = Field(default=4, ge=1, le=8)


@workflows.activity(name="read-graduated-mints")
async def read_graduated_mints(
    repo_owner: str,
    repo_name: str,
    branch: str,
    github: ToolCallClient = Depends(github_connector),
) -> list[str]:
    store = GitHubStore(github, repo_owner, repo_name, branch)
    payload = await store.read_json("data/pumpfun/graduated.json")
    return list((payload or {}).get("mints") or [])


@workflows.activity(name="sync-ohlcv-batch")
async def sync_ohlcv_batch(
    mints: list[str],
    timeframe: str,
    candle_limit: int,
    repo_owner: str,
    repo_name: str,
    branch: str,
    github: ToolCallClient = Depends(github_connector),
) -> dict[str, Any]:
    store = GitHubStore(github, repo_owner, repo_name, branch)
    results: dict[str, Any] = {}
    files: list[tuple[str, str]] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for mint in mints:
            pool = await top_pool_address(client, mint)
            if not pool:
                results[mint] = {"status": "no_pool"}
                continue
            candles = await fetch_ohlcv(client, pool, timeframe, candle_limit)
            if not candles:
                results[mint] = {"status": "no_candles"}
                continue
            existing = await store.read_json("data/ohlcv/" + mint + ".json") or {}
            by_time = {c["t"]: c for c in (existing.get("candles") or [])}
            for candle in candles:
                by_time[candle["t"]] = candle
            merged = sorted(by_time.values(), key=lambda c: c["t"])
            payload = {
                "mint": mint,
                "pool": pool,
                "timeframe": timeframe,
                "candles": merged,
                "candle_count": len(merged),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            files.append(("data/ohlcv/" + mint + ".json", json.dumps(payload)))
            results[mint] = {"status": "ok", "pool": pool, "candles": len(merged)}

    if files:
        await store.push(
            files, "ohlcv-sync: updated " + str(len(files)) + " tokens"
        )
    return results


@workflows.workflow.define(
    name="ohlcv-sync",
    workflow_display_name="GeckoTerminal OHLCV Sync",
    workflow_description=(
        "Fetches OHLCV history for graduated tokens from the free "
        "GeckoTerminal API and stores it in the GitHub data lake."
    ),
    on_behalf_of=True,
)
@uses_connectors(github_connector)
class OhlcvSyncWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, params: OhlcvSyncParams) -> dict[str, Any]:
        mints = await read_graduated_mints(
            params.repo_owner, params.repo_name, params.branch
        )
        if not mints:
            return {"status": "no_graduated_tokens", "synced": {}}
        recent = mints[-params.max_tokens:]
        synced: dict[str, Any] = {}
        for start in range(0, len(recent), params.batch_size):
            batch = recent[start : start + params.batch_size]
            batch_results = await sync_ohlcv_batch(
                batch,
                params.timeframe,
                params.candle_limit,
                params.repo_owner,
                params.repo_name,
                params.branch,
            )
            synced.update(batch_results)
        return {
            "status": "ok",
            "tokens_considered": len(recent),
            "synced": synced,
        }
