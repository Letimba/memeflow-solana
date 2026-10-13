"""Workflow: pumpfun-ingest.

Captures the free PumpPortal event streams (new launches, graduations)
and appends them to the GitHub data lake. Designed to run on a schedule;
each execution listens for listen_minutes and then persists one commit.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

import mistralai.workflows as workflows
from mistralai.workflows import Depends
from mistralai.workflows.plugins.mistralai.connectors import (
    ToolCallClient,
    uses_connectors,
)

from src.lib.pumpportal import listen
from src.lib.storage import GitHubStore, github_connector

NL = chr(10)


class PumpfunIngestParams(BaseModel):
    repo_owner: str
    repo_name: str
    branch: str = "main"
    listen_minutes: float = Field(default=10.0, gt=0.0, le=55.0)
    chunk_seconds: int = Field(default=120, ge=10, le=300)


@workflows.activity(name="pumpportal-listen-chunk")
async def pumpportal_listen_chunk(seconds: int) -> list[dict[str, Any]]:
    """Listen to PumpPortal for the given seconds and return raw events."""
    events: list[dict[str, Any]] = []

    def on_event(message: dict[str, Any]) -> None:
        record = dict(message)
        record["received_at"] = datetime.now(timezone.utc).isoformat()
        events.append(record)

    await listen(seconds, on_event)
    return events


@workflows.activity(name="persist-pumpfun-events")
async def persist_pumpfun_events(
    events: list[dict[str, Any]],
    repo_owner: str,
    repo_name: str,
    branch: str,
    github: ToolCallClient = Depends(github_connector),
) -> dict[str, Any]:
    """Append captured events to the data lake and update the graduated list."""
    store = GitHubStore(github, repo_owner, repo_name, branch)
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")

    def to_jsonl(records: list[dict[str, Any]]) -> str:
        return NL.join(json.dumps(r, sort_keys=True) for r in records)

    files: list[tuple[str, str]] = []
    new_tokens = [e for e in events if e.get("event_type") == "new_token"]
    migrations = [e for e in events if e.get("event_type") == "migration"]

    if new_tokens:
        path = "data/pumpfun/launches/" + day + ".jsonl"
        existing = await store.read_jsonl(path)
        files.append((path, to_jsonl(existing + new_tokens)))

    if migrations:
        path = "data/pumpfun/migrations/" + day + ".jsonl"
        existing = await store.read_jsonl(path)
        files.append((path, to_jsonl(existing + migrations)))

        graduated = await store.read_json("data/pumpfun/graduated.json")
        mints: list[str] = list((graduated or {}).get("mints") or [])
        known = set(mints)
        for event in migrations:
            mint = event.get("mint")
            if isinstance(mint, str) and mint not in known:
                mints.append(mint)
                known.add(mint)
        payload = {
            "mints": mints[-5000:],
            "count": len(mints[-5000:]),
            "updated_at": now.isoformat(),
        }
        files.append(
            ("data/pumpfun/graduated.json", json.dumps(payload, indent=2))
        )

    if files:
        await store.push(
            files,
            "pumpfun-ingest: "
            + str(len(new_tokens))
            + " launches, "
            + str(len(migrations))
            + " graduations ("
            + day
            + ")",
        )

    return {
        "status": "ok",
        "events_captured": len(events),
        "new_tokens": len(new_tokens),
        "migrations": len(migrations),
        "files_written": [path for path, _ in files],
    }


@workflows.workflow.define(
    name="pumpfun-ingest",
    workflow_display_name="Pump.fun Event Ingest",
    workflow_description=(
        "Listens to the free PumpPortal streams (new launches and graduations) "
        "and appends them to the GitHub data lake."
    ),
    on_behalf_of=True,
)
@uses_connectors(github_connector)
class PumpfunIngestWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, params: PumpfunIngestParams) -> dict[str, Any]:
        budget_seconds = int(params.listen_minutes * 60)
        collected: list[dict[str, Any]] = []
        elapsed = 0
        while elapsed < budget_seconds:
            chunk = min(params.chunk_seconds, budget_seconds - elapsed)
            events = await pumpportal_listen_chunk(chunk)
            collected.extend(events)
            elapsed += chunk
        return await persist_pumpfun_events(
            collected, params.repo_owner, params.repo_name, params.branch
        )
