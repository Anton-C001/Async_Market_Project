"""
Two phases:

  1. bootstrap()  — one REST call per symbol to /api/v3/klines to pin the
                    reference price at the start of the look-back window.
  2. stream()     — one persistent websocket carrying a combined stream for
                    every symbol; each tick updates that symbol's growth
                    versus its reference price.

No API key is needed as both endpoints are public market data.

Dependencies:  pip install aiohttp websockets
Run:           python binance_stream.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import aiohttp
import websockets

REST_BASE = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/stream"

# Binance drops every websocket connection after 24h and expects a pong
# for each ping within 60s. The `websockets` library answers pings for us;
# the reconnect loop in stream() handles the 24h cut and network blips.
MAX_BACKOFF_SECONDS = 60

log = logging.getLogger("binance_stream")


@dataclass
class Tracker:
    """Live state for one symbol."""

    symbol: str
    reference: float             # price at the start of the look-back window
    last: float | None = None    # most recent price seen on the stream
    ticks: int = 0

    @property
    def growth(self) -> float | None:
        """Fractional change since the reference price (0.05 == +5 %)."""
        if self.last is None:
            return None
        return self.last / self.reference - 1.0


# --------------------------------------------------------------------------
# Phase 1: REST bootstrap
# --------------------------------------------------------------------------

async def fetch_reference_price(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    lookback: int,
) -> float:
    """
    Return the open price of the first candle in the look-back window.

    A kline row from Binance is:
      [open_time, open, high, low, close, volume, close_time, quote_vol, ...]
    Prices come back as strings, hence the float() casts.
    """
    params = {"symbol": symbol, "interval": interval, "limit": lookback}
    async with session.get(f"{REST_BASE}/api/v3/klines", params=params) as resp:
        resp.raise_for_status()
        rows = await resp.json()

    if not rows:
        raise ValueError(f"no kline data returned for {symbol}")

    return float(rows[0][1])


async def bootstrap(
    symbols: list[str],
    interval: str = "1d",
    lookback: int = 30,
) -> dict[str, Tracker]:
    """
    Fetch reference prices for all symbols concurrently.

    asyncio.gather fires every request at once and waits for them all,
    so N symbols cost roughly one round-trip rather than N.
    """
    async with aiohttp.ClientSession() as session:
        prices = await asyncio.gather(
            *(fetch_reference_price(session, s, interval, lookback) for s in symbols)
        )

    trackers = {s: Tracker(symbol=s, reference=p) for s, p in zip(symbols, prices)}
    for t in trackers.values():
        log.info("bootstrap %-10s reference=%.6g (%s x %d)", t.symbol, t.reference, interval, lookback)
    return trackers


# --------------------------------------------------------------------------
# Phase 2: websocket stream
# --------------------------------------------------------------------------

def combined_stream_url(symbols: list[str], stream_type: str = "miniTicker") -> str:
    """
    Build the combined-stream URL, e.g.
      wss://stream.binance.com:9443/stream?streams=btcusdt@miniTicker/ethusdt@miniTicker

    Stream names must be lower-case. Combined-stream messages arrive wrapped as
      {"stream": "btcusdt@miniTicker", "data": {...}}
    """
    names = "/".join(f"{s.lower()}@{stream_type}" for s in symbols)
    return f"{WS_BASE}?streams={names}"


def handle_message(raw: str | bytes, trackers: dict[str, Tracker]) -> Tracker | None:
    """
    Decode one combined-stream frame and update the matching tracker.

    miniTicker payload fields:  s = symbol, c = latest close (price),
                                o = open, h = high, l = low, v = volume, E = event time
    Returns the updated tracker, or None for frames we don't track.
    """
    msg = json.loads(raw)
    data = msg.get("data")
    if not data:              # e.g. a subscription ack or an error frame
        log.debug("non-data frame: %s", msg)
        return None

    tracker = trackers.get(data.get("s"))
    if tracker is None:
        return None

    tracker.last = float(data["c"])
    tracker.ticks += 1
    return tracker


async def stream(trackers: dict[str, Tracker]) -> None:
    """
    Hold the websocket open, reconnecting with exponential backoff.

    Cancel the task (or Ctrl-C the process) to stop.
    """
    url = combined_stream_url(list(trackers))
    backoff = 1.0

    while True:
        try:
            log.info("connecting %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                backoff = 1.0  # healthy connection: reset the backoff
                async for raw in ws:
                    try:
                        tracker = handle_message(raw, trackers)
                    except Exception:
                        log.exception("bad frame, skipping")
                        continue

        except asyncio.CancelledError:
            log.info("stream cancelled, closing")
            raise

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
            log.warning("connection lost (%s); reconnecting in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

def render(trackers: dict[str, Tracker]) -> None:
    """
    Render a table of all trackers to the terminal, sorted by growth.
    """

    rows = sorted(trackers.values(), key=lambda t: t.growth or 0, reverse=True)
    # print("\033[2J\033[H", end="")  # clear screen, cursor to top
    print('******')
    for t in rows:
        if t.last is None:
            print(f"{t.symbol:<10} {'-':>14}  {'-':>8}")
        else:
            print(f"{t.symbol:<10} {t.last:>14.6g}  {t.growth:+8.2%}")

async def render_loop(
    trackers: dict[str, Tracker],
    interval: float = 1.0,
) -> None:
    """
    Render the table every `interval` seconds. Called concurrently with stream().
    """
    while True:
        render(trackers)
        await asyncio.sleep(interval)


# --------------------------------------------------------------------------
# Main program
# --------------------------------------------------------------------------

def print_update(t: Tracker) -> None:
    print(f"{time.strftime('%H:%M:%S')}  {t.symbol:<10} {t.last:>14.6g}  {t.growth:+8.2%}  ({t.ticks} ticks)")


# Previous version: render() was called on every tick, which is a lot of output.
#
# async def main() -> None:
#     symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
#     trackers = await bootstrap(symbols, interval="1d", lookback=30)
#     await asyncio.gather(
#         stream(trackers, on_update=render)  # no-op; render the whole table instead of per-tick
#     )

# Current version: render() is called on a timer, so the table is updated every few seconds.
async def main() -> None:
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
    trackers = await bootstrap(symbols, interval="1d", lookback=30)

    await asyncio.gather(
        stream(trackers),  # no-op; render the whole table instead of per-tick
        render_loop(trackers, interval=3.0),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass