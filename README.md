# MemeFlow — Solana Pump.fun Data, Backtesting & Optimization

Production-grade data infrastructure for Pump.fun meme-coin strategies, built on **Mistral Workflows**. MemeFlow captures live token launches and migrations, builds OHLCV history via GeckoTerminal, runs cost-aware event-driven backtests, and optimizes parameters with walk-forward grid search — end to end, fully automated, with zero paid data dependencies.

## Highlights

- **Free data sources**: PumpPortal WebSocket streams (token launches, Raydium migrations) + GeckoTerminal keyless REST API (OHLCV, ~27 calls/min pacing built in).
- **Zero-cost data lake**: All data is stored as JSON/JSONL in a GitHub repository — versioned, diffable, and free.
- **Cost-aware backtesting**: Slippage (bps) and pool fees on both sides, priority fee (SOL) and Jito tip (SOL) subtracted from every trade. Conservative same-candle rule: stop-loss is evaluated before take-profit.
- **Walk-forward optimization**: Train/test split grid search with an explicit `train_to_test_decay` overfitting metric.
- **Connector-based security**: No secrets in code. GitHub access resolves at runtime via the Mistral `github_app` connector with `on_behalf_of=True` (requires a hardened deployment).
- **Deterministic workflows**: WebSocket listening runs in bounded activity chunks (default 120 s) so workflow code stays deterministic and replay-safe.

## Workflows

| Workflow | Purpose | Key inputs | Output
|---|---|---|---
| `pumpfun-ingest` | Listen to PumpPortal WS: new token launches + Raydium migrations | `repo_owner`, `repo_name`, `listen_minutes`, `chunk_seconds` | `data/launches.jsonl`, `data/migrations.jsonl`, `data/graduated.json`
| `ohlcv-sync` | Fetch/refresh hourly OHLCV per graduated token via GeckoTerminal | `repo_owner`, `repo_name`, `timeframe` (minutes), `max_tokens`, `batch_size` | `data/ohlcv/{mint}.json` (dedup by timestamp)
| `backtest-strategy` | Cost-aware event-driven backtest over the OHLCV dataset | `repo_owner`, `repo_name`, `timeframe`, strategy + costs params | Metrics: win rate, PnL, expectancy, max drawdown, trade counts
| `optimize-strategy` | Walk-forward grid search (train/test split, decay metric) | `repo_owner`, `repo_name`, `timeframe`, param grid | Ranked parameter sets + best-params JSON

## Architecture

```
PumpPortal WS ──► pumpfun-ingest ──► data/launches.jsonl, graduated.json
                                        │
GeckoTerminal REST ◄── ohlcv-sync ◄─────┘
        │
        ▼
data/ohlcv/{mint}.json ──► backtest-strategy ──► optimize-strategy
```

All state lives in the Git data lake; workers are stateless and horizontally scalable.

## Data Schema

- `data/launches.jsonl` — one JSON per Pump.fun launch: mint, name, symbol, creator, timestamp, bonding curve key.
- `data/migrations.jsonl` — one JSON per Raydium migration: mint, pool/bonding curve, timestamp, token amounts.
- `data/graduated.json` — deduplicated map of graduated mints.
- `data/ohlcv/{mint}.json` — `{"mint": "...", "timeframe": "60", "candles": [{"t": iso, "o": ..., "h": ..., "l": ..., "c": ..., "volume": ...}]}`

## Setup

Requirements:

- Mistral account with **pay-as-you-go billing** enabled (managed workflow deployments require it).
- The Mistral GitHub App installed on your repo with **write** access (repo is used as the data lake).
- Python 3.12 worker image (provided by this repo `Dockerfile`).

Deploy (hardened mode is required because workflows use on-behalf-of connector credentials):

```bash
curl -X POST https://api.mistral.ai/v1/workflow-deployments \
  -H "Authorization: Bearer $MISTRAL_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "deployment_name": "memeflow-solana",
    "github_url": "https://github.com/Letimba/memeflow-solana",
    "revision": "main",
    "hardened": true
  }'}'
```

Or from Mistral Studio / via the Mistral MCP tools (`deploy_workflow_deployment` → `wait_workflow_deployment_ready`).

## Recommended Schedules

```json
// pumpfun-ingest — every 15 minutes
{ "workflow_identifier": "pumpfun-ingest",
  "schedule": { "intervals": [{"every": "PT15M"}],
    "input": {"repo_owner":"Letimba","repo_name":"memeflow-solana","listen_minutes":12,"chunk_seconds":120} } }

// ohlcv-sync — hourly
{ "workflow_identifier": "ohlcv-sync",
  "schedule": { "intervals": [{"every": "PT1H"}],
    "input": {"repo_owner":"Letimba","repo_name":"memeflow-solana","timeframe":"60","max_tokens":12} } }
```

## Example: Run a Backtest

```json
{
  "repo_owner": "Letimba",
  "repo_name": "memeflow-solana",
  "timeframe": "60",
  "strategy": {
    "momentum_window": 3,
    "entry_momentum_pct": 10.0,
    "take_profit_pct": 80.0,
    "stop_loss_pct": 20.0,
    "trailing_stop_pct": 10.0
  },
  "costs": {
    "slippage_bps": 150,
    "pool_fee_bps": 100,
    "priority_fee_sol": 0.0001,
    "jito_tip_sol": 0.0001
  }
}
```

The result reports per-token and aggregate metrics: number of trades, win rate, gross/net PnL, expectancy, max drawdown, and more.

## Commercial Packaging

MemeFlow is built to be sold as a service:

- **Starter** — one deployment, shared data lake, daily OHLCV sync, monthly optimization report.
- **Pro** — dedicated deployment, 15-min ingest cadence, hourly sync, walk-forward optimizer access, webhook/alert handoff.
- **Quant** — custom parameter grids, multi-timeframe support, strategy co-development and private benchmarking.

## Disclaimer

Meme coins are extremely volatile and high risk. Backtest results — even cost-aware, walk-forward validated ones — do not guarantee future performance. This software is for research purposes and is not financial advice.

## License

Proprietary — © 2026 MemeFlow. Contact the repo owner for commercial licensing.
