import asyncio
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Protocol

from shared.logger import log


def now_ms() -> int:
    return int(time.time() * 1000)


def percentile(sorted_vals: list[int], p: float) -> float:
    """Calculate the percentile of a list of values."""
    if not sorted_vals:
        return 0.0
    k = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    k = max(0, min(k, len(sorted_vals) - 1))
    return float(sorted_vals[k])


@dataclass
class SymbolStats:
    """Per-symbol statistics tracker."""
    prices: deque = field(default_factory=lambda: deque(maxlen=200))
    sizes: deque = field(default_factory=lambda: deque(maxlen=200))
    returns: deque = field(default_factory=lambda: deque(maxlen=100))
    last_price: float = 0.0
    event_count: int = 0
    volume_total: float = 0.0


class BusLike(Protocol):
    """A protocol for the bus.
    Args:
        drops: The number of drops.
        subs: The list of subscribers.
    """

    drops: int
    subs: list[asyncio.Queue]

    def subscribe(self, maxsize: int = 1000) -> asyncio.Queue: ...

    """Subscribe to the bus.
    Args:
        maxsize: The maximum size of the queue.
    Returns:
        The queue.
    """

    def queue_depths(self) -> list[int]: ...

    """Get the queue depths.
    Returns:
        The queue depths.
    """


async def consumer_vwap(bus: BusLike, window_n: int = 200, print_every_s: float = 5.0):
    """Computes rolling VWAP per symbol and tracks message latency."""
    q = bus.subscribe(maxsize=1000)

    # Rolling window of (price, size) tuples per symbol
    windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=window_n))
    ages_ms: deque[int] = deque(maxlen=3000)  # Latency: exchange -> now
    pipes_ms: deque[int] = deque(maxlen=3000)  # Latency: recv -> now
    last_print = time.time()

    while True:
        try:
            e = await q.get()
            if not isinstance(e, dict):
                continue

            try:
                symbol = e.get("product_id", "UNKNOWN")
                price = float(e.get("price", 0.0))
                size = float(e.get("last_size", 0.0))
                ingest_ts = int(e.get("ingest_ts_ms", 0))  # When exchange sent the msg
                recv_ts = int(e.get("recv_ts_ms", 0)) if e.get("recv_ts_ms") else 0  # When we received it
            except Exception:
                continue

            if price <= 0 or size < 0 or ingest_ts <= 0:
                continue

            windows[symbol].append((price, size))

            # Track latency metrics
            now = now_ms()
            ages_ms.append(max(0, now - ingest_ts))  # Total age from exchange
            if recv_ts > 0:
                pipes_ms.append(max(0, now - recv_ts))  # Processing lag in pipeline

            if (time.time() - last_print) >= print_every_s:
                # VWAP = sum(price * size) / sum(size) for each symbol
                vwaps = []
                for sym, window in sorted(windows.items()):
                    if window:
                        num = sum(p * s for p, s in window)  # Price-weighted sum
                        den = sum(s for _, s in window)  # Total size
                        vwap = (num / den) if den > 0 else 0.0
                        vwaps.append(f"{sym}={vwap:.2f}")

                ages_sorted = sorted(ages_ms)
                pipes_sorted = sorted(pipes_ms)

                log.log(
                    "VWAP",
                    f"{' | '.join(vwaps)} | "
                    f"age p99={percentile(ages_sorted, 99):.0f}ms | "
                    f"pipe p99={percentile(pipes_sorted, 99):.0f}ms | drops={bus.drops}"
                )
                last_print = time.time()

        except Exception as ex:
            log.error(f"Error in consumer_vwap: {ex}")


async def consumer_health(bus: BusLike, print_every_s: float = 5.0):
    """Monitors event throughput, queue backpressure, and overall system health."""
    q = bus.subscribe(maxsize=1000)
    last_print = time.time()
    count = 0
    last_price = None

    while True:
        try:
            e = await q.get()
            count += 1
            # Track most recent price for health check
            try:
                if isinstance(e, dict) and e.get("price") is not None:
                    last_price = float(e.get("price", last_price))
            except Exception:
                pass

            now = time.time()
            if (now - last_print) >= print_every_s:
                dt = now - last_print
                eps = (count / dt) if dt > 0 else 0.0  # Events/sec throughput
                depths = bus.queue_depths()  # Check for queue buildup

                log.log(
                    "HEALTH",
                    f"eps={eps:.1f} | price={last_price} | "
                    f"drops={bus.drops} | subs={len(bus.subs)} | qdepths={depths}"
                )
                count = 0
                last_print = now

        except Exception as ex:
            log.error(f"Error in consumer_health: {ex}")


async def consumer_volatility(bus: BusLike, window_n: int = 100, print_every_s: float = 10.0):
    """Computes annualized volatility per symbol using log returns."""
    q = bus.subscribe(maxsize=1000)

    # Track last price and rolling log returns per symbol
    last_prices: dict[str, float] = {}
    returns: dict[str, deque] = defaultdict(lambda: deque(maxlen=window_n))
    last_print = time.time()

    while True:
        try:
            e = await q.get()
            if not isinstance(e, dict):
                continue

            try:
                symbol = e.get("product_id", "UNKNOWN")
                price = float(e.get("price", 0.0))
            except Exception:
                continue

            if price <= 0:
                continue

            # Log return = ln(P_t / P_{t-1}) - better for compounding
            if symbol in last_prices and last_prices[symbol] > 0:
                log_return = math.log(price / last_prices[symbol])
                returns[symbol].append(log_return)

            last_prices[symbol] = price

            if (time.time() - last_print) >= print_every_s:
                vols = []
                for sym in sorted(returns.keys()):
                    r = returns[sym]
                    if len(r) >= 10:
                        # Sample variance of log returns
                        mean_r = sum(r) / len(r)
                        var = sum((x - mean_r) ** 2 for x in r) / len(r)
                        std = math.sqrt(var) if var > 0 else 0
                        # Annualize: assumes 1 tick/sec → 86400 ticks/day → 31.5M ticks/yr
                        annual_vol = std * math.sqrt(86400 * 365) * 100
                        vols.append(f"{sym}={annual_vol:.1f}%")

                if vols:
                    log.log("VOLATILITY", f"{' | '.join(vols)}")
                last_print = time.time()

        except Exception as ex:
            log.error(f"Error in consumer_volatility: {ex}")


async def consumer_volume(bus: BusLike, print_every_s: float = 10.0):
    """Tracks USD trading volume and trade count per symbol."""
    q = bus.subscribe(maxsize=1000)

    # Accumulate volume (USD) and trade count per symbol
    volumes: dict[str, float] = defaultdict(float)
    trades: dict[str, int] = defaultdict(int)
    last_print = time.time()
    window_start = time.time()

    while True:
        try:
            e = await q.get()
            if not isinstance(e, dict):
                continue

            try:
                symbol = e.get("product_id", "UNKNOWN")
                price = float(e.get("price", 0.0))
                size = float(e.get("last_size", 0.0))
            except Exception:
                continue

            if price <= 0 or size <= 0:
                continue

            volumes[symbol] += size * price  # Notional USD volume
            trades[symbol] += 1

            now = time.time()
            if (now - last_print) >= print_every_s:
                window_secs = now - window_start
                vol_strs = []
                for sym in sorted(volumes.keys()):
                    vol_usd = volumes[sym]
                    trade_count = trades[sym]
                    # Normalize to volume/minute for easier comparison
                    vol_per_min = (vol_usd / window_secs) * 60 if window_secs > 0 else 0
                    vol_strs.append(f"{sym}=${vol_per_min/1000:.1f}K/min({trade_count}tx)")

                if vol_strs:
                    log.log("VOLUME", f"{' | '.join(vol_strs)}")

                # Reset accumulators for next window
                volumes.clear()
                trades.clear()
                window_start = now
                last_print = now

        except Exception as ex:
            log.error(f"Error in consumer_volume: {ex}")
