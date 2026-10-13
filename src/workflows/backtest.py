"""Workflow: backtest-strategy.

Runs a parameterized, cost-aware backtest over the collected OHLCV data.
All execution-relevant Solana costs (slippage, pool fee, priority fee,
Jito tip) are part of the model.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

import mistralai.workflows as workflows
from mistralai.workflows import Depends
from mistralai.workflows.plugins.mistralai.connectors import (
    ToolCallClient,
    uses_connectors,
)

from src.lib.backtest_engine import run_backtest
from src.lib.storage import GitHubStore, github_connector


class Costs(BaseModel):
    slippage_bps: float = 50.0
    pool_fee_bps: float = 25.0
    priority_fee_sol: float = 0.0005
    jito_tip_sol: float = 0.0001


class Strategy(BaseModel):
    momentum_window: int = 3
    entry_momentum_pct: float = 5.0
    entry_min_volume_usd: float = 0.0
    take_profit_pct: float = 40.0
    stop_loss_pct: float = 15.0
    trailing_stop_pct: float = 0.0
    max_hold_candles: int = 24
    position_sol: float = 0.1


class BacktestParams(BaseModel):
    repo_owner: str
    repo_name: str
    branch: str = "main"
    timeframe: str = "60"
    max_tokens: int = Field(default=50, ge=1, le=200)
    costs: Costs = Field(default_factory=Costs)
    strategy: Strategy = Field(default_factory=Strategy)


@workflows.activity(name="load-ohlcv-dataset")
async def load_ohlcv_dataset(
    repo_owner: str,
    repo_name: str,
    branch: str,
    timeframe: str,
    max_tokens: int,
    github: ToolCallClient = Depends(github_connector),
) -> dict[str, list[dict[str, Any]]]:
    store = GitHubStore(github, repo_owner, repo_name, branch)
    names = [n for n in await store.list_files("data/ohlcv") if n.endswith(".json")]
    if not names:
        return {}
    if max_tokens and len(names) > max_tokens:
        names = names[-max_tokens:]
    dataset: dict[str, list[dict[str, Any]]] = {}
    for name in names:
        payload = await store.read_json("data/ohlcv/" + name)
        if not isinstance(payload, dict):
            continue
        if payload.get("timeframe") != timeframe:
            continue
        candles = payload.get("candles") or []
        if candles:
            dataset[name[:-5]] = candles
    return dataset


@workflows.activity(name="run-backtest")
async def run_backtest_activity(
    dataset: dict[str, list[dict[str, Any]]],
    strategy: dict[str, Any],
    costs: dict[str, Any],
) -> dict[str, Any]:
    return run_backtest(dataset, strategy, costs)


@workflows.workflow.define(
    name="backtest-strategy",
    workflow_display_name="Meme-Coin Strategy Backtest",
    workflow_description=(
        "Runs a cost-aware backtest over collected OHLCV data and returns "
        "trade-level results and summary metrics."
    ),
    on_behalf_of=True,
)
@uses_connectors(github_connector)
class BacktestWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, params: BacktestParams) -> dict[str, Any]:
        dataset = await load_ohlcv_dataset(
            params.repo_owner,
            params.repo_name,
            params.branch,
            params.timeframe,
            params.max_tokens,
        )
        if not dataset:
            return {
                "status": "no_data",
                "hint": "Run ohlcv-sync first, or wait until pumpfun-ingest captured graduations.",
            }
        result = await run_backtest_activity(
            dataset,
            params.strategy.model_dump(),
            params.costs.model_dump(),
        )
        result["status"] = "ok"
        result["tokens"] = len(dataset)
        return result
