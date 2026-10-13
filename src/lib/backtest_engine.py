"""Event-driven backtest engine with realistic Solana execution costs.

Cost model per round trip:
  - slippage (bps, both sides) + pool fee (bps, both sides)
  - fixed priority fee (SOL) + Jito tip (SOL) per trade
Exit rules are evaluated conservatively: if both stop-loss and
take-profit touch within one candle, the stop-loss wins.
"""
from __future__ import annotations

from typing import Any, Iterable

DEFAULT_STRATEGY: dict[str, Any] = {
    "momentum_window": 3,
    "entry_momentum_pct": 5.0,
    "entry_min_volume_usd": 0.0,
    "take_profit_pct": 40.0,
    "stop_loss_pct": 15.0,
    "trailing_stop_pct": 0.0,
    "max_hold_candles": 24,
    "position_sol": 0.1,
}

DEFAULT_COSTS: dict[str, Any] = {
    "slippage_bps": 50.0,
    "pool_fee_bps": 25.0,
    "priority_fee_sol": 0.0005,
    "jito_tip_sol": 0.0001,
}


def _counts(values: Iterable[Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        key = str(value)
        out[key] = out.get(key, 0) + 1
    return out


def _simulate_token(
    mint: str, candles: list[dict[str, Any]], params: dict[str, Any], costs: dict[str, Any]
) -> list[dict[str, Any]]:
    ordered = sorted(candles or [], key=lambda c: c.get("t", 0))
    window = int(params["momentum_window"])
    if len(ordered) < window + 2:
        return []

    entry_mom = float(params["entry_momentum_pct"]) / 100.0
    min_vol = float(params.get("entry_min_volume_usd", 0.0))
    tp = float(params["take_profit_pct"]) / 100.0
    sl = float(params["stop_loss_pct"]) / 100.0
    trail = float(params.get("trailing_stop_pct", 0.0)) / 100.0
    max_hold = int(params["max_hold_candles"])
    position = float(params["position_sol"])
    round_trip_cost = (
        2.0 * (float(costs["slippage_bps"]) + float(costs["pool_fee_bps"])) / 10000.0
    )
    fixed_cost = float(costs["priority_fee_sol"]) + float(costs["jito_tip_sol"])

    trades: list[dict[str, Any]] = []
    i = window
    while i < len(ordered) - 1:
        signal = ordered[i]
        reference = ordered[i - window]
        ref_close = float(reference.get("c") or 0.0)
        sig_close = float(signal.get("c") or 0.0)
        if ref_close <= 0.0 or sig_close <= 0.0:
            i += 1
            continue
        momentum = sig_close / ref_close - 1.0
        volume = float(signal.get("v") or 0.0)
        if momentum < entry_mom or volume < min_vol:
            i += 1
            continue

        entry_index = i + 1
        entry = float(ordered[entry_index].get("o") or 0.0)
        if entry <= 0.0:
            i += 1
            continue

        peak_close = entry
        exit_price = None
        exit_reason = "timeout"
        exit_index = entry_index
        hold = 0
        j = entry_index
        while j < len(ordered) and hold < max_hold:
            candle = ordered[j]
            hold += 1
            low = float(candle.get("l") or 0.0)
            high = float(candle.get("h") or 0.0)
            close = float(candle.get("c") or 0.0)
            if low <= entry * (1.0 - sl):
                exit_price = entry * (1.0 - sl)
                exit_reason = "stop_loss"
                exit_index = j
                break
            if high >= entry * (1.0 + tp):
                exit_price = entry * (1.0 + tp)
                exit_reason = "take_profit"
                exit_index = j
                break
            if trail > 0.0 and close > 0.0:
                peak_close = max(peak_close, close)
                if close <= peak_close * (1.0 - trail):
                    exit_price = close
                    exit_reason = "trailing_stop"
                    exit_index = j
                    break
            j += 1

        if exit_price is None:
            exit_index = min(j, len(ordered) - 1)
            exit_price = float(ordered[exit_index].get("c") or entry)

        gross = exit_price / entry - 1.0
        net_pct = gross - round_trip_cost
        net_pnl = position * net_pct - fixed_cost
        trades.append(
            {
                "mint": mint,
                "entry_time": ordered[entry_index].get("t"),
                "exit_time": ordered[exit_index].get("t"),
                "entry_price": entry,
                "exit_price": exit_price,
                "gross_pct": round(gross * 100.0, 4),
                "net_pct": round(net_pct * 100.0, 4),
                "net_pnl_sol": round(net_pnl, 8),
                "hold_candles": hold,
                "exit_reason": exit_reason,
            }
        )
        i = exit_index + 1
    return trades


def _summarize(trades: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(trades)
    if n == 0:
        return {
            "n_trades": 0,
            "note": "No entries matched the strategy filter in the provided dataset.",
        }
    wins = [t for t in trades if t["net_pnl_sol"] > 0]
    losses = [t for t in trades if t["net_pnl_sol"] <= 0]
    gross_win = sum(t["net_pnl_sol"] for t in wins)
    gross_loss = abs(sum(t["net_pnl_sol"] for t in losses))
    total = sum(t["net_pnl_sol"] for t in trades)

    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for trade in trades:
        cumulative += trade["net_pnl_sol"]
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)

    return {
        "n_trades": n,
        "win_rate": round(len(wins) / n, 4),
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss > 0 else None,
        "expectancy_sol": round(total / n, 8),
        "total_pnl_sol": round(total, 8),
        "max_drawdown_sol": round(drawdown, 8),
        "avg_net_pct": round(sum(t["net_pct"] for t in trades) / n, 6),
        "avg_hold_candles": round(sum(t["hold_candles"] for t in trades) / n, 2),
        "exit_reasons": _counts(t["exit_reason"] for t in trades),
        "trades_preview": trades[:50],
        "trades_total": n,
    }


def run_backtest(
    dataset: dict[str, list[dict[str, Any]]],
    strategy: dict[str, Any] | None,
    costs: dict[str, Any] | None,
) -> dict[str, Any]:
    params = dict(DEFAULT_STRATEGY)
    params.update(strategy or {})
    cost_model = dict(DEFAULT_COSTS)
    cost_model.update(costs or {})

    trades: list[dict[str, Any]] = []
    for mint, candles in (dataset or {}).items():
        trades.extend(_simulate_token(mint, candles, params, cost_model))
    trades.sort(key=lambda t: (t.get("exit_time") or 0, t.get("mint") or ""))

    summary = _summarize(trades)
    summary["strategy"] = params
    summary["costs"] = cost_model
    return summary
