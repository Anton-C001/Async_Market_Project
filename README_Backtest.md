A backtester for simple long/flat trading strategies on Binance historical candles, 
with: paged REST download of any length of history, a one-line contract for adding
strategies, fills at the next bar's open, fees, and a summary table per symbol.

No API key required.

Example Output:
```
BTCUSDT  2024-01-24 -> 2026-10-19  (1000 bars)
STRATEGY                RETURN      CAGR  SHARPE   MAX DD  EXPOSURE  TRADES   WIN %
buy_and_hold           +104.2%    +29.8%    0.79   -53.0%      100%       1    100%
donchian_breakout       +25.5%     +8.6%    0.43   -38.0%       47%      16     38%
sma_crossover           +15.8%     +5.5%    0.33   -38.6%       54%      12     50%
bollinger_reversion     +15.5%     +5.4%    0.33   -38.3%       22%      17     76%
rsi_reversion            +9.7%     +3.4%    0.26   -27.4%       22%       6     33%
```

## What it does

Replays a set of strategies over past candles and reports how each would have done,
considering fees, against buy-and-hold.

1. **Fetch** — `/api/v3/klines` serves at most 1000 candles per request, so longer
   histories are paged backwards until `--bars` candles are in hand. The
   still-forming candle is dropped so nothing is ever decided on an unfinished bar.
   All symbols are fetched concurrently, the same way `Binance_Stream.py` bootstraps.
2. **Signal** — each strategy is a function from the candle DataFrame to a *target
   position* per bar: `1.0` fully long, `0.0` flat, decided at that bar's close.
3. **Simulate** — `backtest()` fills each target at the *next* bar's open, charges a
   fee on whatever changed hands, marks equity at every close, and summarises the
   equity curve.
4. **Render** — one table per symbol, best total return first; `--plot` adds equity
   curves.

## Running it

```bash
python -m pip install aiohttp pandas numpy matplotlib
python Backtest.py
python Backtest.py --symbols BTCUSDT ETHUSDT --interval 4h --bars 3000 --plot
```

Requires Python 3.10+. `matplotlib` is only needed for plotting.

| Flag | Default | Meaning |
|---|---|---|
| `--symbols` | `BTCUSDT ETHUSDT SOLUSDT` | Binance spot pairs |
| `--interval` | `1h` | candle size, any the klines endpoint accepts (`1m` … `1w`) |
| `--bars` | `6000` | number of closed candles of history |
| `--fee` | `0.001` | fee per side as a fraction of notional (0.1 %, the Binance spot default) |
| `--strategies` | all | subset of the registered strategies to run |
| `--plot` | off | equity curves, one panel per symbol, log scale |

The plot opens in a separate matplotlib window; nothing is drawn in the terminal.
As the code currently stands `main()` also calls `plot()` after every symbol
regardless of `--plot` (a convenience for running inside VS Code) — comment that
line out if you only want the table.

Alternatively you can bring the file into your IDE and change the default parameters in the following 
code block:
```python
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
```

## Reading the table

| Column | Meaning |
|---|---|
| RETURN | total return over the whole window, net of fees |
| CAGR | the same return annualised, using a 365-day year |
| SHARPE | annualised mean / std of per-bar returns, risk-free rate taken as 0 |
| MAX DD | worst peak-to-trough fall in equity |
| EXPOSURE | fraction of bars spent in the market |
| TRADES | round trips; a position still open at the end counts, marked at the final close |
| WIN % | fraction of round trips that made money after fees |

## Strategies

All are long/flat; none have been tuned, the parameters are textbook defaults.

| Name | Rule |
|---|---|
| `buy_and_hold` | always long — the benchmark the others have to beat |
| `sma_crossover` | long while the 20-bar average is above the 50-bar average |
| `rsi_reversion` | buy when RSI(14) drops below 30, sell once it recovers above 55 |
| `donchian_breakout` | buy a close above the prior 20-bar high, sell a close below the prior 10-bar low |
| `bollinger_reversion` | buy a close below the lower 2σ band, sell back at the 20-bar mean |

### Adding one

Write a function that takes the candle DataFrame (`open high low close volume`,
indexed by UTC open time) and returns a `Series` of positions, then register it:

```python
def my_strategy(df: pd.DataFrame) -> pd.Series:
    ...
    return latch(enter=some_condition, exit=other_condition)

STRATEGIES["my_strategy"] = my_strategy
```

It then appears in the table and in `--strategies`. The one rule: a position at bar
*t* may use rows up to and including *t*, never later ones. `rolling()` and `shift()`
respect that by construction; anything that fits a model to the whole series does not.
`latch()` turns separate enter/exit conditions into a held position, and `rsi()` is 
there to reuse.

## Design notes

The decisions worth explaining, and what was rejected:

**Fills at the next bar's open, not the signal close.** A strategy decides at a
bar's close, so it cannot also trade at that close — the price would be known before
the order existed. Each bar is simulated as two legs: the old position rides the gap
from the previous close to this open, the fee is paid at the open on whatever changed
hands, and the new position rides open to close. On a bar with no trade the two legs
collapse to the plain close-to-close return. On 24/7 crypto the next open is almost
always within a tick of the signal close, so the numbers barely move; the point is
that the engine is honest by construction rather than by luck.

**Multiplicative, not additive, accounting.** Equity compounds as
`(1 + prev·gap) · (1 − fee·turnover) · (1 + held·intrabar)` per bar, so a long run of
small returns and fees is exact rather than approximated.

**Closed candles only.** The last candle Binance returns is still forming; its close
changes every tick. It is dropped at fetch time, so a run at 11:59 and a run at 12:01
differ by exactly one bar, not by a half-finished one.

**A 365-day year.** Crypto trades every day, so annualising with 252 (the equity
convention) would overstate CAGR and understate Sharpe. The bars-per-year figure is
derived from the interval, so `4h` and `1d` runs are directly comparable.

**Flat during warm-up.** A 50-bar moving average is `NaN` for the first 49 bars.
`NaN` compares `False`, so strategies are flat until they have enough history rather
than taking a position on garbage.

**Strategies are plain functions, not classes.** There is no state to carry between
bars that a `Series` can't hold, and a function is the smallest thing that can be
tested in isolation or swapped in by name.

## Known limitations

- No slippage: fills are at the exact open. Add a few basis points to `--fee` to
  approximate it.
- No shorting, no leverage, no position sizing — a strategy is in or out.
- A single bad symbol fails the whole fetch (`asyncio.gather` is all-or-nothing).
- Asking for more history than a symbol has silently returns what exists; the
  table header shows the actual span.
- Fees in `trade_returns` are applied to the price (`buy·(1+fee)`) and in the equity
  curve to the notional (`×(1−fee)`); the two differ at second order, invisibly.

## Possible extensions

- `--plot-file equity.png` to save curves instead of opening a window
- Slippage as a separate parameter, and partial fills via fractional positions
- Cache candles to Parquet so repeated runs don't re-download
- Parameter sweeps (`sma_crossover` over a grid of fast/slow) with a heat-map

## References

- [Binance spot REST API — klines](https://developers.binance.com/docs/binance-spot-api-docs/rest-api#klinecandlestick-data)
- [Binance spot trading fees](https://www.binance.com/en/fee/schedule)
