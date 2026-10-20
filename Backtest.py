"""
Backtest simple long/flat trading strategies on Binance historical candles.

Three phases:

  1. fetch_candles()  — page through /api/v3/klines (same public endpoint
                        Binance_Stream.py bootstraps from) into a DataFrame.
  2. strategies       — each one maps candles to a target position per bar:
                        1.0 = fully long, 0.0 = flat.
  3. backtest()       — turn target positions into an equity curve, net of
                        fees, and summarise it (return, Sharpe, drawdown...).

No API key is needed as the klines endpoint is public market data.

Dependencies:  pip install aiohttp pandas numpy matplotlib
               (matplotlib is only needed for --plot)
Run:           python Backtest.py
               python Backtest.py --symbols BTCUSDT ETHUSDT --interval 4h --bars 3000 --plot
"""

from __future__ import annotations

import argparse
import asyncio
import time
from dataclasses import dataclass
from typing import Callable

import aiohttp
import numpy as np
import pandas as pd

REST_BASE = "https://api.binance.com"
MAX_KLINES_PER_REQUEST = 1000   # hard limit on /api/v3/klines

# Crypto trades 24/7, so a year is 365 days of bars (not 252 as for equities).
SECONDS_PER_YEAR = 365 * 24 * 60 * 60
INTERVAL_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
    "1d": 86400, "3d": 259200, "1w": 604800,
}


# --------------------------------------------------------------------------
# Phase 1: historical data
# --------------------------------------------------------------------------

async def fetch_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    bars: int,
) -> pd.DataFrame:
    """
    Return the last `bars` *closed* candles for `symbol`, oldest first.

    Binance serves at most 1000 klines per request, so longer histories are
    paged backwards: each request ends just before the oldest candle so far.

    A kline row from Binance is:
      [open_time, open, high, low, close, volume, close_time, quote_vol, ...]
    """
    rows: list[list] = []
    end_time: int | None = None

    while len(rows) < bars:
        # +1 on the first page leaves room to drop the still-forming candle
        want = bars - len(rows) + (1 if end_time is None else 0)
        params = {"symbol": symbol, "interval": interval, "limit": min(want, MAX_KLINES_PER_REQUEST)}
        if end_time is not None:
            params["endTime"] = end_time

        async with session.get(f"{REST_BASE}/api/v3/klines", params=params) as resp:
            resp.raise_for_status()
            page = await resp.json()
            # page is a list of klines oldest first.
            # for the first page, the last kline is still forming and its close_time is in the future so drop it
            # the rest of the pages only contain closed candles so keep them all

        if end_time is None:
            # a candle whose close_time is in the future hasn't finished yet;
            # trading on it would be look-ahead
            now_ms = time.time() * 1000
            page = [r for r in page if r[6] < now_ms]
        if not page:
            break  # ran out of history (symbol listed more recently than requested)

        rows = page + rows
        end_time = page[0][0] - 1

    if not rows:
        raise ValueError(f"no kline data returned for {symbol}")

    df = pd.DataFrame(
        [r[:6] for r in rows[-bars:]],
        columns=["open_time", "open", "high", "low", "close", "volume"],
    )
    df.index = pd.to_datetime(df.pop("open_time"), unit="ms", utc=True)
    return df.astype(float)   # prices come back as strings


async def fetch_all(symbols: list[str], interval: str, bars: int) -> dict[str, pd.DataFrame]:
    """Fetch history for all symbols concurrently."""
    async with aiohttp.ClientSession() as session:
        frames = await asyncio.gather(
            *(fetch_candles(session, s, interval, bars) for s in symbols)
        )
    return dict(zip(symbols, frames))


# --------------------------------------------------------------------------
# Phase 2: strategies
# --------------------------------------------------------------------------
#
# A strategy takes the candle DataFrame and returns the *target position* for
# each bar (1.0 long, 0.0 flat), decided using data up to and including that
# bar's close. backtest() fills it at the *next* bar's open, so a strategy
# may freely use the current row but must never peek at later ones.

Strategy = Callable[[pd.DataFrame], pd.Series]


def latch(enter: pd.Series, exit: pd.Series) -> pd.Series:
    """
    Turn entry/exit conditions into a held position: go long when `enter`
    fires, stay long until `exit` fires. Exit wins if both fire together.
    """
    position = pd.Series(float("nan"), index=enter.index)
    position[enter] = 1.0
    position[exit] = 0.0
    return position.ffill().fillna(0.0)


def buy_and_hold(df: pd.DataFrame) -> pd.Series:
    """Always long. The benchmark every other strategy has to beat."""
    return pd.Series(1.0, index=df.index)


def sma_crossover(df: pd.DataFrame, fast: int = 20, slow: int = 50) -> pd.Series:
    """Trend following: long while the fast moving average is above the slow one."""
    fast_ma = df["close"].rolling(fast).mean()
    slow_ma = df["close"].rolling(slow).mean()
    return (fast_ma > slow_ma).astype(float)   # NaN warm-up compares False -> flat


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's Relative Strength Index, 0-100."""
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, min_periods=period).mean()
    return 100 - 100 / (1 + gain / loss)


def rsi_reversion(df: pd.DataFrame, period: int = 14, buy_below: float = 30, sell_above: float = 55) -> pd.Series:
    """Mean reversion: buy when oversold, sell once RSI has recovered."""
    r = rsi(df["close"], period)
    return latch(enter=r < buy_below, exit=r > sell_above)


def donchian_breakout(df: pd.DataFrame, entry: int = 20, exit: int = 10) -> pd.Series:
    """
    Breakout: buy a close above the prior `entry`-bar high, sell a close
    below the prior `exit`-bar low. shift(1) keeps the current bar out of
    its own channel — otherwise a close could never exceed the high.
    """
    upper = df["high"].rolling(entry).max().shift(1)
    lower = df["low"].rolling(exit).min().shift(1)
    return latch(enter=df["close"] > upper, exit=df["close"] < lower)


def bollinger_reversion(df: pd.DataFrame, window: int = 20, width: float = 2.0) -> pd.Series:
    """Mean reversion: buy a close below the lower band, sell back at the middle band."""
    mid = df["close"].rolling(window).mean()
    lower = mid - width * df["close"].rolling(window).std()
    return latch(enter=df["close"] < lower, exit=df["close"] > mid)


# Add a strategy here and it shows up in the results table and --strategies.
STRATEGIES: dict[str, Strategy] = {
    "buy_and_hold": buy_and_hold,
    "sma_crossover": sma_crossover,
    "rsi_reversion": rsi_reversion,
    "donchian_breakout": donchian_breakout,
    "bollinger_reversion": bollinger_reversion,
}


# --------------------------------------------------------------------------
# Phase 3: backtest engine
# --------------------------------------------------------------------------

@dataclass
class Result:
    """Outcome of one strategy on one symbol."""

    name: str
    equity: pd.Series        # growth of 1 unit of capital, net of fees
    total_return: float      # 0.05 == +5 %
    cagr: float              # annualised return
    sharpe: float            # annualised, risk-free rate taken as 0
    max_drawdown: float      # worst peak-to-trough fall, as a negative fraction
    exposure: float          # fraction of bars spent in the market
    trades: int              # round trips (an open position at the end counts)
    win_rate: float          # fraction of trades that made money after fees


def trade_returns(df: pd.DataFrame, held: pd.Series, fee: float) -> list[float]:
    """
    Net return of each round trip, for long/flat strategies.

    `held` is the position carried through each bar, so a change in it is a
    fill at that bar's open. A position still open on the last bar is marked
    at the final close.
    """
    change = held.diff().fillna(held)   # first bar: flat -> held
    entries = df["open"][change > 0].tolist()
    exits = df["open"][change < 0].tolist()
    if len(exits) < len(entries):
        exits.append(df["close"].iloc[-1])
    return [(sell * (1 - fee)) / (buy * (1 + fee)) - 1 for buy, sell in zip(entries, exits)]


def backtest(
    df: pd.DataFrame,
    strategy: Strategy,
    name: str,
    fee: float = 0.01,
    periods_per_year: float = 365,
) -> Result:
    """
    Simulate `strategy` on `df` and summarise the result.

    A signal produced at bar t's close is filled at bar t+1's open: the
    strategy can't trade on the very price it just used to decide. Each
    bar is then two legs — the old position rides the gap from the previous
    close to this open, the new position rides this open to this close —
    and the fee is paid at the open on whatever changed hands.

    Equity is marked to market at every close. `fee` is charged per side as
    a fraction of traded notional (0.001 = the 0.1 % Binance spot default).
    Slippage is not modelled.
    """
    target = strategy(df).clip(0.0, 1.0)

    held = target.shift(1).fillna(0.0)            # position from this bar's open onwards
    prev = held.shift(1).fillna(0.0)              # position until this bar's open
    gap = (df["open"] / df["close"].shift(1) - 1).fillna(0.0)
    intrabar = df["close"] / df["open"] - 1
    turnover = (held - prev).abs()
    growth = (1 + prev * gap) * (1 - fee * turnover) * (1 + held * intrabar)
    net = growth - 1

    equity = growth.cumprod()
    total_return = equity.iloc[-1] - 1
    years = len(df) / periods_per_year
    cagr = equity.iloc[-1] ** (1 / years) - 1 if years > 0 else float("nan")

    vol = net.std()
    sharpe = net.mean() / vol * np.sqrt(periods_per_year) if vol > 0 else 0.0
    max_drawdown = (equity / equity.cummax() - 1).min()

    trades = trade_returns(df, held, fee)
    win_rate = sum(r > 0 for r in trades) / len(trades) if trades else float("nan")

    return Result(
        name=name,
        equity=equity,
        total_return=total_return,
        cagr=cagr,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
        exposure=held.mean(),
        trades=len(trades),
        win_rate=win_rate,
    )


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def render(symbol: str, df: pd.DataFrame, results: list[Result]) -> None:
    """Print one table per symbol, best total return first."""
    span = f"{df.index[0]:%Y-%m-%d} -> {df.index[-1]:%Y-%m-%d}"
    print(f"\n{symbol}  {span}  ({len(df)} bars)")
    print(f"{'STRATEGY':<20} {'RETURN':>9} {'CAGR':>9} {'SHARPE':>7} {'MAX DD':>8} {'EXPOSURE':>9} {'TRADES':>7} {'WIN %':>7}")
    for r in sorted(results, key=lambda r: r.total_return, reverse=True):
        win = "-" if np.isnan(r.win_rate) else f"{r.win_rate:.0%}"
        print(
            f"{r.name:<20} {r.total_return:>+9.1%} {r.cagr:>+9.1%} {r.sharpe:>7.2f} "
            f"{r.max_drawdown:>8.1%} {r.exposure:>9.0%} {r.trades:>7} {win:>7}"
        )


def plot(all_results: dict[str, list[Result]]) -> None:
    """Equity curves, one panel per symbol."""
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(len(all_results), 1, figsize=(11, 4 * len(all_results)), squeeze=False)
    for ax, (symbol, results) in zip(axes[:, 0], all_results.items()):
        for r in results:
            ax.plot(r.equity.index, r.equity, label=r.name, linewidth=1.2)
        ax.set_title(f"{symbol} — growth of 1 unit, net of fees")
        ax.set_yscale("log")
        ax.grid(alpha=0.3)
        ax.legend()
    fig.tight_layout()
    plt.show()


# --------------------------------------------------------------------------
# Main program
# --------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest trading strategies on Binance candles.")
    p.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    p.add_argument("--interval", default="1h", choices=list(INTERVAL_SECONDS))
    p.add_argument("--bars", type=int, default=6000, help="number of candles of history")
    p.add_argument("--fee", type=float, default=0.001, help="fee per side as a fraction (0.001 = 0.1%%)")
    p.add_argument("--strategies", nargs="+", default=list(STRATEGIES), choices=list(STRATEGIES), metavar="NAME",
                   help=f"subset to run; available: {', '.join(STRATEGIES)}")
    p.add_argument("--plot", action="store_true", help="show equity curves (needs matplotlib)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    periods_per_year = SECONDS_PER_YEAR / INTERVAL_SECONDS[args.interval]

    candles = asyncio.run(fetch_all(args.symbols, args.interval, args.bars))
    print()
    all_results: dict[str, list[Result]] = {}
    for symbol, df in candles.items():
        all_results[symbol] = [
            backtest(df, STRATEGIES[name], name, fee=args.fee, periods_per_year=periods_per_year)
            for name in args.strategies
        ]
        render(symbol, df, all_results[symbol])

        plot(all_results) # This is here so that results can be plotted when the program is run in vscode, instead of the terminal. Comment out if you don't want to plot results.

    if args.plot:
        plot(all_results)


if __name__ == "__main__":
    main()
