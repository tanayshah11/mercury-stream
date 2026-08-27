#!/usr/bin/env python3
"""
Stress testing tool for MercuryStream.

Generates synthetic events at high volume and measures throughput/latency.

Usage:
    python stress.py --duration 30 --rate 5000
    python stress.py --count 100000 --rate 0
"""

import asyncio
import os
import random
import struct
import sys
import time
from dataclasses import dataclass, field

import typer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orjson

from services.shared import log

P2_HOST = os.getenv("P2_HOST", "localhost")
P2_PORT = int(os.getenv("P2_PORT", "9001"))

SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]


def now_ms() -> int:
    """Current Unix timestamp in milliseconds"""
    return int(time.time() * 1000)


def frame(payload: bytes) -> bytes:
    """Prefix payload with 4-byte big-endian length header (P2 protocol)"""
    return struct.pack(">I", len(payload)) + payload


@dataclass
class Stats:
    """Track sent events, errors, and throughput"""
    sent: int = 0
    errors: int = 0
    start_time: float = 0.0

    def record_send(self) -> None:
        """Increment sent count"""
        self.sent += 1

    def throughput(self) -> float:
        """Calculate events per second"""
        elapsed = time.monotonic() - self.start_time
        return self.sent / elapsed if elapsed > 0 else 0

    def report(self) -> str:
        elapsed = time.monotonic() - self.start_time
        return (
            f"sent={self.sent} | "
            f"errors={self.errors} | "
            f"throughput={self.throughput():.0f}/s | "
            f"elapsed={elapsed:.1f}s"
        )
        # Note: Pipeline latency is measured by the consumer, not here.
        # We only measure sender-side throughput.


class EventGenerator:
    """Generates synthetic events with per-symbol sequential trade_ids."""

    def __init__(self, feeder_id: str = "stress-0", base_trade_id: int = 900_000_000):
        self.feeder_id = feeder_id
        # Track trade_id per symbol to avoid false gap detection
        self.trade_ids: dict[str, int] = {sym: base_trade_id for sym in SYMBOLS}

    def generate(self, symbol: str | None = None) -> dict:
        """Generate synthetic market event with realistic price movements"""
        sym = symbol or random.choice(SYMBOLS)
        base_price = {"BTC-USD": 95000, "ETH-USD": 3500, "SOL-USD": 200}.get(sym, 100)
        # Add small gaussian noise (0.1% std dev) to simulate price fluctuations
        price = base_price * (1 + random.gauss(0, 0.001))
        # Exponential distribution for trade sizes (realistic for order book)
        size = random.expovariate(1) * 0.1

        # Get next trade_id for this symbol (sequential per symbol)
        trade_id = self.trade_ids.get(sym, 900_000_000)
        self.trade_ids[sym] = trade_id + 1

        # Generate realistic bid-ask spread (0.01-0.1% of price)
        spread = price * random.uniform(0.0001, 0.001)
        best_bid = price - spread / 2
        best_ask = price + spread / 2

        # Generate 24h stats with realistic bounds
        open_24h = price * (1 + random.gauss(0, 0.02))  # 2% std dev from current
        high_24h = max(price, open_24h) * (1 + random.uniform(0, 0.03))
        low_24h = min(price, open_24h) * (1 - random.uniform(0, 0.03))

        # Realistic daily volume (varies by asset)
        volume_scale = {"BTC-USD": 10000, "ETH-USD": 50000, "SOL-USD": 100000}.get(sym, 10000)
        volume_24h = random.uniform(0.5, 1.5) * volume_scale

        return {
            "type": "ticker",
            "product_id": sym,
            "price": round(price, 2),
            "last_size": round(size, 8),
            "side": random.choice(["buy", "sell"]),
            "time": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime()),
            "trade_id": trade_id,
            "sequence": trade_id,  # Use same as trade_id for consistency
            # 24-hour stats
            "open_24h": round(open_24h, 2),
            "high_24h": round(high_24h, 2),
            "low_24h": round(low_24h, 2),
            "volume_24h": round(volume_24h, 2),
            "volume_30d": round(volume_24h * 30, 2),  # Approximate
            # Top-of-book
            "best_bid": round(best_bid, 2),
            "best_bid_size": round(random.expovariate(1) * 0.5, 8),
            "best_ask": round(best_ask, 2),
            "best_ask_size": round(random.expovariate(1) * 0.5, 8),
            "feeder_id": self.feeder_id,
            "ingest_ts_ms": now_ms(),
        }


class RateLimiter:
    """Simple interval-based rate limiter for accurate throughput control"""
    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.interval = 1.0 / rate if rate > 0 else 0  # Time between events
        self.next_time = time.monotonic()  # When we can send next

    async def acquire(self) -> None:
        """Wait until it's time to send the next event"""
        if self.rate <= 0:  # Unlimited rate
            return

        now = time.monotonic()
        if now < self.next_time:
            # Not yet time - wait for the remaining interval
            await asyncio.sleep(self.next_time - now)

        # If we fell behind (next_time is in the past), catch up
        if self.next_time < time.monotonic():
            self.next_time = time.monotonic()

        # Schedule next send time (from target, not actual, to prevent drift)
        self.next_time += self.interval


async def stress_test(
    host: str,
    port: int,
    rate: float,
    duration: float | None,
    count: int | None,
    symbol: str | None,
    batch_size: int,
    connection_id: int = 0,  # Unique ID per connection to avoid duplicate sequences
) -> Stats:
    """Send events to P2 via TCP connection, respecting rate limits"""
    feeder_id = f"stress-{connection_id}"
    # Each connection gets unique base trade_id range to avoid collisions
    # Use 10M spacing to support up to 100 connections safely (vs 100M which only supports 10)
    base_trade_id = 900_000_000 + (connection_id * 10_000_000)
    generator = EventGenerator(feeder_id=feeder_id, base_trade_id=base_trade_id)

    stats = Stats()
    stats.start_time = time.monotonic()

    log.info(f"Connecting to {host}:{port}")
    try:
        _, writer = await asyncio.open_connection(host, port)
    except (OSError, ConnectionRefusedError) as e:
        log.error(f"Connection failed: {e}")
        return stats

    rate_limiter = RateLimiter(rate)
    last_report = time.monotonic()

    def should_continue() -> bool:
        """Check stopping condition: duration elapsed or count reached"""
        if duration is not None:
            return (time.monotonic() - stats.start_time) < duration
        if count is not None:
            return stats.sent < count
        return True

    try:
        while should_continue():
            # Process events in batches to reduce loop overhead
            for _ in range(batch_size):
                if not should_continue():
                    break

                # Rate limit BEFORE sending to control throughput accurately
                if rate > 0:
                    await rate_limiter.acquire()

                event = generator.generate(symbol)

                try:
                    # Serialize and send: orjson -> frame with length prefix -> TCP
                    payload = orjson.dumps(event)
                    writer.write(frame(payload))
                    await writer.drain()
                    stats.record_send()
                except (BrokenPipeError, ConnectionResetError):
                    stats.errors += 1
                    raise

            # Log stats every 2 seconds
            now = time.monotonic()
            if (now - last_report) >= 2.0:
                log.info(stats.report())
                last_report = now

    except (BrokenPipeError, ConnectionResetError) as e:
        log.error(f"Connection lost: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

    return stats


async def stress_parallel(
    host: str,
    port: int,
    rate: float,
    duration: float | None,
    count: int | None,
    symbol: str | None,
    connections: int,
) -> None:
    """Spawn multiple parallel connections to scale load testing"""
    # Divide rate and count across connections
    per_conn_rate = rate / connections if rate > 0 else 0
    per_conn_count = count // connections if count else None

    log.info(f"Starting {connections} parallel connections")
    log.info(f"Per-connection rate: {per_conn_rate:.0f}/s")

    tasks = [
        stress_test(
            host=host,
            port=port,
            rate=per_conn_rate,
            duration=duration,
            count=per_conn_count,
            symbol=symbol,
            batch_size=10,
            connection_id=i,  # Unique ID for each connection
        )
        for i in range(connections)
    ]

    # Wait for all connections to complete
    results = await asyncio.gather(*tasks)

    # Aggregate stats across all connections
    total_sent = sum(s.sent for s in results)
    total_errors = sum(s.errors for s in results)
    elapsed = time.monotonic() - results[0].start_time if results else 0

    log.info("=" * 60)
    log.info("STRESS TEST COMPLETE")
    log.info("=" * 60)
    log.info(f"Connections:  {connections}")
    log.info(f"Total sent:   {total_sent:,}")
    log.info(f"Total errors: {total_errors}")
    log.info(f"Duration:     {elapsed:.1f}s")
    log.info(f"Throughput:   {total_sent / elapsed:,.0f}/s" if elapsed > 0 else "N/A")
    log.info("(Pipeline latency is measured by consumers, check processor logs)")


app = typer.Typer()


@app.command()
def main(
    rate: float = typer.Option(
        1000,
        "-r",
        "--rate",
        help="Events per second (0 = unlimited)",
    ),
    duration: float | None = typer.Option(
        None,
        "-t",
        "--duration",
        help="Test duration in seconds",
    ),
    count: int | None = typer.Option(
        None,
        "-n",
        "--count",
        help="Total events to send",
    ),
    connections: int = typer.Option(
        1,
        "-c",
        "--connections",
        help="Number of parallel connections",
    ),
    symbol: str | None = typer.Option(
        None,
        "-s",
        "--symbol",
        help="Symbol to use (default: random)",
    ),
    host: str = typer.Option(
        P2_HOST,
        "--host",
        help=f"P2 host (default: {P2_HOST})",
    ),
    port: int = typer.Option(
        P2_PORT,
        "--port",
        help=f"P2 port (default: {P2_PORT})",
    ),
) -> None:
    """MercuryStream stress tester"""
    # Default to 10s duration if neither duration nor count specified
    if duration is None and count is None:
        duration = 10.0

    log.info(f"Stress test: rate={rate}/s duration={duration}s count={count}")

    try:
        asyncio.run(
            stress_parallel(
                host=host,
                port=port,
                rate=rate,
                duration=duration,
                count=count,
                symbol=symbol,
                connections=connections,
            )
        )
    except KeyboardInterrupt:
        log.info("Stress test interrupted")


if __name__ == "__main__":
    app()
