"""Workflow: optimize-strategy.

Walk-forward grid search: every parameter combination is ranked on a
train split of the candles, and the top configs are re-evaluated on the
held-out test split. The train-to-test expectancy decay is reported so
overfitting is visible, not hidden.
"""
from __future__ import annotations

import itertools
from typing import Any

from pydantic import BaseModel, Field

import mistralai.workflows as workflows
from mistralai.workflows.plugins.mistralai.connectors import uses_connectors

from src.lib.backtest_engine import run_backtest
from src.lib.storage import github_connector
from src.workflows.backtest import Costs, load_ohlcv_dataset


class OptimizeParams(BaseModel):
    repo_owner: str
    repo_name: str
    branch: str = "main"
    timeframe: str = "60"
    max_tokens: int = Field(default=50, ge=1, le=200)
    train_fraction: float = Field(default=0.7, gt=0.1, le=0.9)
    top_k: int = Field(default=10, ge=1, le=50)
    min_trades: int = Field(default=5, ge=1)
    costs: Costs = Field(default_factory=Costs)
    grid: dict[str, list[Any]] = Field(
        default_factory=lambda: {
            "momentum_window": [2, 3, 6],
            "entry_momentum_pct": [3.0, 10.0],
            "take_profit_pct": [30.0, 80.0],
            "stop_loss_pct": [10.0, 30.0],
            "trailing_stop_pct": [0.0, 10.0],
        }
    )


@workflows.activity(name="grid-search")
async def grid_search_activity(
    dataset: dict[str, list[dict[str, Any]]],
    grid: dict[str, list[Any]],
    costs: dict[str, Any],
    train_fraction: float,
    top_k: int,
    min_trades: int,
) -> dict[str, Any]:
    keys = sorted(key for key in grid if grid.get(key))
    values = [list(grid[key]) for key in keys]

    train: dict[str, list[dict[str, Any]]] = {}
    test: dict[str, list[dict[str, Any]]] = {}
    for mint, candles in dataset.items():
        ordered = sorted(candles, key=lambda c: c["t"])
        cut = int(len(ordered) * train_fraction)
        if cut >= 2 and len(ordered) - cut >= 2:
            train[mint] = ordered[:cut]
            test[mint] = ordered[cut:]
        else:
            train[mint] = ordered

    scored: list[dict[str, Any]] = []
    combos = 0
    for combo in itertools.product(*values):
        override = dict(zip(keys, combo))
        combos += 1
        result = run_backtest(train, override, costs)
        if result["n_trades"] >= min_trades:
            scored.append({"params": override, "train": result})

    scored.sort(
        key=lambda s: (
            s["train"]["expectancy_sol"],
            s["train"]["profit_factor"] or 0.0,
        ),
        reverse=True,
    )
    top = scored[:top_k]
    for entry in top:
        entry["test"] = run_backtest(test, entry["params"], costs)
        train_expectancy = entry["train"].get("expectancy_sol") or 0.0
        test_expectancy = entry["test"].get("expectancy_sol") or 0.0
        entry["train_to_test_decay"] = (
            round(test_expectancy / train_expectancy, 4)
            if train_expectancy > 0
            else None
        )

    return {
        "status": "ok",
        "combos_evaluated": combos,
        "configs_with_min_trades": len(scored),
        "train_tokens": len(train),
        "test_tokens": len(test),
        "top_configs": top,
        "method": (
            "walk-forward: rank on train split, re-evaluate on held-out "
            "test split; train_to_test_decay < 0.5 signals overfitting"
        ),
    }


@workflows.workflow.define(
    name="optimize-strategy",
    workflow_display_name="Walk-Forward Strategy Optimizer",
    workflow_description=(
        "Grid-searches strategy parameters with a train/test walk-forward "
        "split and reports overfitting-aware rankings."
    ),
    on_behalf_of=True,
)
@uses_connectors(github_connector)
class OptimizeWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, params: OptimizeParams) -> dict[str, Any]:
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
        return await grid_search_activity(
            dataset,
            params.grid,
            params.costs.model_dump(),
            params.train_fraction,
            params.top_k,
            params.min_trades,
        )
