"""MemeFlow worker entrypoint.

Starts the worker and registers all four service workflows:
  pumpfun-ingest, ohlcv-sync, backtest-strategy, optimize-strategy
"""
from __future__ import annotations

import asyncio

import mistralai.workflows as workflows

from src.workflows.backtest import BacktestWorkflow
from src.workflows.ohlcv_sync import OhlcvSyncWorkflow
from src.workflows.optimize import OptimizeWorkflow
from src.workflows.pumpfun_ingest import PumpfunIngestWorkflow


async def main() -> None:
    await workflows.run_worker(
        [
            PumpfunIngestWorkflow,
            OhlcvSyncWorkflow,
            BacktestWorkflow,
            OptimizeWorkflow,
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
