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
    """Track sent events, errors, and latency distributions"""
    sent: int = 0
    errors: int = 0
    start_time: float = 0.0
    latencies: list[float] = field(default_factory=list)

    def record_send(self, latency_ms: float) -> None:
        """Increment sent count and cap latency samples at 100k to prevent memory bloat"""
        self.sent += 1
        if len(self.latencies) < 100000:
            self.latencies.append(latency_ms)

    def throughput(self) -> float:
        """Calculate events per second"""
        elapsed = time.monotonic() - self.start_time
        return self.sent / elapsed if elapsed > 0 else 0

    def percentile(self, p: float) -> float:
        """Calculate percentile from latency samples (p=50 for median, p=99 for p99)"""
        if not self.latencies:
            return 0.0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * p / 100)
        idx = min(idx, len(sorted_lat) - 1)
        return sorted_lat[idx]

    def report(self) -> str:
        elapsed = time.monotonic() - self.start_time
        return (
            f"sent={self.sent} | "
            f"errors={self.errors} | "
            f"throughput={self.throughput():.0f}/s | "
            f"p50={self.percentile(50):.2f}ms | "
            f"p95={self.percentile(95):.2f}ms | "
            f"p99={self.percentile(99):.2f}ms | "
            f"elapsed={elapsed:.1f}s"
        )


def generate_event(seq: int, symbol: str | None = None) -> dict:
    """Generate synthetic market event with realistic price movements"""
    sym = symbol or random.choice(SYMBOLS)
    base_price = {"BTC-USD": 95000, "ETH-USD": 3500, "SOL-USD": 200}.get(sym, 100)
    # Add small gaussian noise (0.1% std dev) to simulate price fluctuations
    price = base_price * (1 + random.gauss(0, 0.001))
    # Exponential distribution for trade sizes (realistic for order book)
    size = random.expovariate(1) * 0.1

    return {
        "type": "ticker",
        "product_id": sym,
        "price": round(price, 2),
        "last_size": round(size, 8),
        "time": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime()),
        "trade_id": 900000000 + seq,
        "sequence": seq,
        "ingest_ts_ms": now_ms(),
    }


class RateLimiter:
    """Token bucket rate limiter to control event throughput"""
    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.tokens = 0.0  # Available tokens
        self.last_time = time.monotonic()

    async def acquire(self) -> None:
        """Block until a token is available, implementing token bucket algorithm"""
        if self.rate <= 0:  # Unlimited rate
            return

        now = time.monotonic()
        elapsed = now - self.last_time
        # Refill tokens based on elapsed time
        self.tokens += elapsed * self.rate
        self.tokens = min(self.tokens, 10.0)  # Cap burst to 10 tokens max
        self.last_time = now

        if self.tokens < 1.0:
            # Not enough tokens, sleep until we have one
            wait_time = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait_time)
            self.tokens = 0.0
        else:
            # Consume one token
            self.tokens -= 1.0


async def stress_test(
    host: str,
    port: int,
    rate: float,
    duration: float | None,
    count: int | None,
    symbol: str | None,
    batch_size: int,
) -> Stats:
    """Send events to P2 via TCP connection, respecting rate limits"""
    stats = Stats()
    stats.start_time = time.monotonic()

    log.info(f"Connecting to {host}:{port}")
    try:
        _, writer = await asyncio.open_connection(host, port)
    except (OSError, ConnectionRefusedError) as e:
        log.error(f"Connection failed: {e}")
        return stats

    rate_limiter = RateLimiter(rate)
    seq = 0
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
            batch_start = time.monotonic()

            # Process events in batches to reduce loop overhead
            for _ in range(batch_size):
                if not should_continue():
                    break

                event = generate_event(seq, symbol)
                seq += 1

                send_start = time.monotonic()
                try:
                    # Serialize and send: orjson -> frame with length prefix -> TCP
                    payload = orjson.dumps(event)
                    writer.write(frame(payload))
                    await writer.drain()  # Ensure sent before measuring latency
                    latency = (time.monotonic() - send_start) * 1000
                    stats.record_send(latency)
                except (BrokenPipeError, ConnectionResetError):
                    stats.errors += 1
                    raise

                if rate > 0:
                    await rate_limiter.acquire()

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
        )
        for _ in range(connections)
    ]

    # Wait for all connections to complete
    results = await asyncio.gather(*tasks)

    # Aggregate stats across all connections
    total_sent = sum(s.sent for s in results)
    total_errors = sum(s.errors for s in results)
    all_latencies = []
    for s in results:
        all_latencies.extend(s.latencies)

    elapsed = time.monotonic() - results[0].start_time if results else 0

    log.info("=" * 60)
    log.info("STRESS TEST COMPLETE")
    log.info("=" * 60)
    log.info(f"Connections:  {connections}")
    log.info(f"Total sent:   {total_sent:,}")
    log.info(f"Total errors: {total_errors}")
    log.info(f"Duration:     {elapsed:.1f}s")
    log.info(f"Throughput:   {total_sent / elapsed:,.0f}/s" if elapsed > 0 else "N/A")

    if all_latencies:
        sorted_lat = sorted(all_latencies)
        # Calculate percentiles from aggregated latencies
        p50 = sorted_lat[int(len(sorted_lat) * 0.50)]
        p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
        p99 = sorted_lat[min(int(len(sorted_lat) * 0.99), len(sorted_lat) - 1)]
        log.info(f"Latency p50:  {p50:.2f}ms")
        log.info(f"Latency p95:  {p95:.2f}ms")
        log.info(f"Latency p99:  {p99:.2f}ms")


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
