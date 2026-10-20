This is an Asyncio client for Binance's public websocket feed with: concurrent REST bootstrap, live growth tracking across multiple symbols, automatic reconnect with exponential backoff.

No API key required because both endpoints used here are public market data.

```
SYMBOL              PRICE    GROWTH
BTCUSDT           43512.6   +12.44%
ETHUSDT           2284.11    +8.02%
SOLUSDT            98.4471    -3.15%
BNBUSDT            312.850    -5.77%
```

## What it does

Tracks how far a set of Binance trading pairs have moved over a look-back window,
and keeps that figure current from a live feed rather than by polling.

1. **Bootstrap** — one REST call per symbol to `/api/v3/klines` fixes the reference
   price at the start of the window (default: the open of the first of the last 30
   daily candles). All calls are issued concurrently, so the time taken for N symbols is 
   the time taken for the longest round-trip rather than N.
2. **Stream** — a single websocket connection carries a *combined* stream for every
   symbol. Each `miniTicker` message updates that symbol's latest price; growth is
   computed on read as `last / reference - 1`.
3. **Render** — a separate coroutine redraws the table on a timer, independent of
   how fast messages arrive.

## Running it

```bash
python -m pip install -r requirements.txt
python Binance_Stream.py
```

Requires Python 3.10+ (uses `X | None` type syntax). Ctrl-C to stop.

Edit the `symbols` list in `main()` to change which pairs are tracked, and the
`interval` / `lookback` arguments to change the window (`"1h"` × 24, `"1w"` × 12 ... 
any interval the klines endpoint accepts).

## Design notes

The decisions worth explaining, and what was rejected:

**`@miniTicker` rather than `@trade`.** The trade stream emits one message per
executed trade — thousands per second on BTC — and this program only needs the
current price roughly once a second. `miniTicker` gives exactly that at a fraction
of the message volume. `@trade` would be the right choice if the goal were volume
or trade-flow analysis.

**One combined stream rather than one connection per symbol.** Binance supports both.
A connection per symbol means N sockets, N reconnect loops, and N times the
connection-limit pressure for identical information. The combined stream wraps each
payload as `{"stream": ..., "data": ...}`, which the message handler unwraps.

**Reference price from the kline *open*, not the close.** The open of the first
candle in the window is an unambiguous boundary: it is the price at the moment the
window began. Using a close would make "30 days" mean 29 days plus a partial one.

**Growth as a computed property, not a stored field.** `Tracker.growth` derives from
`last` and `reference` on every read, so it cannot go stale the way a cached value
updated in two places eventually does.

**Reads and rendering are decoupled.** The websocket loop's only job is to keep
state current as fast as messages arrive; the render loop shows that state at a rate
a human can read. Coupling them — redrawing on every tick — wastes CPU repainting
frames nobody sees, and would make the display rate a hostage to market activity, plus 
we get to reap the rewards of using async techniques  which is nice.

**Backoff resets only on a healthy connection.** The delay doubles to a 60s ceiling
on consecutive failures and resets to 1s once a connection is established, so a brief
network blip recovers immediately while a sustained outage doesn't result in spamming of
the API.

## Why asyncio

The program spends nearly all its time waiting on the network, which is what asyncio
is for. It buys three things here:

- the bootstrap requests run concurrently instead of serially
- the websocket read loop waits for the next message without blocking anything else,
  so the render loop runs alongside it in the same thread
- the reconnect backoff sleeps without freezing the process

Because only one coroutine runs at a time, the shared `trackers` dictionary needs no
locking — a tick can't land halfway through a render.

## Known limitations

- A single bad symbol fails the whole bootstrap (`asyncio.gather` is all-or-nothing
  by default).
- No persistence — state lives in memory and is lost on exit.

## Possible extensions

- Measure the gap between Binance's event timestamp (`E`) and local receive time, and
  flag symbols whose last update has gone stale
- `SUBSCRIBE` / `UNSUBSCRIBE` over the open socket to add symbols at runtime
- An `asyncio.Queue` between the socket reader and consumers, so a slow consumer can
  never stall the read loop
- Persist ticks to SQLite or Parquet for later analysis

## Layout

```
Binance_Stream.py    bootstrap, stream, render, main program
requirements.txt     aiohttp, websockets
```

## References

- [Binance spot websocket streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Binance spot REST API](https://developers.binance.com/docs/binance-spot-api-docs/rest-api)
