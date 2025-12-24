#!/usr/bin/env python3
"""
Replay tool for MercuryStream incident files and recorded data.

This tool is used to replay events from a JSONL file to a P2 service.
Usage:
    python replay.py --file data/btcusd.jsonl --rate 500
    python replay.py --file data/incidents/123/events.jsonl --rate 100 --shuffle-window 10 --duplicate-rate 0.05
"""

import asyncio
import os
import random
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orjson
import typer

from services.shared import log

P2_HOST = os.getenv("P2_HOST", "localhost")
P2_PORT = int(os.getenv("P2_PORT", "9001"))


def now_ms() -> int:
    """Get current Unix timestamp in milliseconds"""
    return int(time.time() * 1000)


def frame(payload: bytes) -> bytes:
    """Frame payload with 4-byte big-endian length prefix."""
    return struct.pack(">I", len(payload)) + payload


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float) -> None:
        self.rate = rate  # tokens per second
        self.tokens = 0.0  # current token balance
        self.last_time = time.monotonic()

    async def acquire(self) -> None:
        """Block until a token is available based on configured rate"""
        if self.rate <= 0:
            return

        now = time.monotonic()
        elapsed = now - self.last_time
        # Replenish tokens based on elapsed time
        self.tokens += elapsed * self.rate
        self.tokens = min(self.tokens, 10.0)  # cap burst to 10 tokens max
        self.last_time = now

        if self.tokens < 1.0:
            # Need to wait for next token
            wait_time = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait_time)
            self.tokens = 0.0
        else:
            # Consume one token
            self.tokens -= 1.0


def apply_shuffle(events: list[dict], window_size: int) -> list[dict]:
    """Shuffle events within windows of size K to simulate out-of-order delivery"""
    if window_size <= 1:
        return events

    result = []
    # Process events in chunks, randomizing order within each chunk
    for i in range(0, len(events), window_size):
        window = events[i : i + window_size]
        random.shuffle(window)
        result.extend(window)
    return result


def inject_duplicates(events: list[dict], rate: float) -> list[dict]:
    """Inject duplicate events at the given rate to test deduplication logic"""
    if rate <= 0:
        return events

    result = []
    for event in events:
        result.append(event)
        # Probabilistically insert a duplicate immediately after
        if random.random() < rate:
            result.append(event.copy())
    return result


def inject_drift(events: list[dict], rate: float) -> list[dict]:
    """Inject schema drift at the given rate to test validation/error handling"""
    if rate <= 0:
        return events

    # Types of schema violations to inject
    drift_types = [
        "missing_price",
        "missing_type",
        "wrong_price_type",
        "wrong_size_type",
        "extra_field",
        "missing_multiple",
    ]

    result = []
    drift_count = 0
    for event in events:
        if random.random() < rate:
            e = event.copy()
            drift_type = random.choice(drift_types)

            # Apply different kinds of schema corruption
            if drift_type == "missing_price":
                e.pop("price", None)
            elif drift_type == "missing_type":
                e.pop("type", None)
            elif drift_type == "wrong_price_type":
                e["price"] = str(e.get("price", "0"))  # string instead of number
            elif drift_type == "wrong_size_type":
                e["last_size"] = str(e.get("last_size", "0"))  # string instead of number
            elif drift_type == "extra_field":
                e["unexpected_field"] = "drift_test"
                e["another_field"] = 12345
            elif drift_type == "missing_multiple":
                e.pop("price", None)
                e.pop("last_size", None)

            result.append(e)
            drift_count += 1
        else:
            result.append(event)

    return result


async def replay(
    file_path: str,
    rate: float = 0,
    shuffle_window: int = 0,
    duplicate_rate: float = 0.0,
    drift_rate: float = 0.0,
    update_timestamps: bool = True,
    host: str = P2_HOST,
    port: int = P2_PORT,
) -> None:
    """Load events from JSONL file, apply chaos testing transforms, and stream to P2 service"""

    if not os.path.exists(file_path):
        log.error(f"File not found: {file_path}")
        return

    # Load all events from JSONL file into memory
    log.info(f"Loading events from {file_path}")
    events = []
    with open(file_path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = orjson.loads(line)
                events.append(event)
            except orjson.JSONDecodeError as e:
                log.warning(f"Skipping invalid JSON: {e}")

    if not events:
        log.error("No events to replay")
        return

    log.info(f"Loaded {len(events)} events")

    # Apply chaos engineering transforms to test system resilience
    if shuffle_window > 0:
        log.info(f"Applying shuffle with window size {shuffle_window}")
        events = apply_shuffle(events, shuffle_window)

    if duplicate_rate > 0:
        original_count = len(events)
        events = inject_duplicates(events, duplicate_rate)
        log.info(f"Injected duplicates: {original_count} -> {len(events)} events")

    if drift_rate > 0:
        events = inject_drift(events, drift_rate)
        drift_count = int(len(events) * drift_rate)
        log.info(f"Injected schema drift: ~{drift_count} events")

    # Establish TCP connection to P2 service
    log.info(f"Connecting to P2 at {host}:{port}")
    try:
        _, writer = await asyncio.open_connection(host, port)
    except (OSError, ConnectionRefusedError) as e:
        log.error(f"Failed to connect to P2: {e}")
        return

    rate_limiter = RateLimiter(rate)
    sent = 0
    start_time = time.monotonic()

    # Stream events to P2 with optional rate limiting
    try:
        for event in events:
            # Optionally replace timestamps with current time
            if update_timestamps:
                event["ingest_ts_ms"] = now_ms()

            # Serialize and frame with length prefix for P2 protocol
            payload = orjson.dumps(event)
            writer.write(frame(payload))
            await writer.drain()

            sent += 1
            # Apply rate limiting if configured
            if rate > 0:
                await rate_limiter.acquire()

            # Progress logging every 1k events
            if sent % 1000 == 0:
                elapsed = time.monotonic() - start_time
                actual_rate = sent / elapsed if elapsed > 0 else 0
                log.info(f"Sent {sent}/{len(events)} events ({actual_rate:.1f}/s)")

    except (BrokenPipeError, ConnectionResetError) as e:
        log.error(f"Connection lost: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

    # Final statistics
    elapsed = time.monotonic() - start_time
    actual_rate = sent / elapsed if elapsed > 0 else 0
    log.info(f"Replay complete: {sent} events in {elapsed:.1f}s ({actual_rate:.1f}/s)")


app = typer.Typer()


@app.command()
def main(
    file: str = typer.Option(..., "-f", "--file", help="Path to JSONL file to replay"),
    rate: float = typer.Option(0, "-r", "--rate", help="Events per second (0 = unlimited)"),
    shuffle_window: int = typer.Option(0, "-s", "--shuffle-window", help="Shuffle events within windows of K events"),
    duplicate_rate: float = typer.Option(0.0, "-d", "--duplicate-rate", help="Rate of duplicate injection (0.0-1.0)"),
    drift_rate: float = typer.Option(0.0, "--drift-rate", help="Rate of schema drift injection (0.0-1.0)"),
    no_update_timestamps: bool = typer.Option(False, "--no-update-timestamps", help="Don't update ingest_ts_ms to current time"),
    host: str = typer.Option(P2_HOST, "--host", help=f"P2 host (default: {P2_HOST})"),
    port: int = typer.Option(P2_PORT, "--port", help=f"P2 port (default: {P2_PORT})"),
) -> None:
    """Replay MercuryStream events"""
    log.info(f"Starting replay: file={file} rate={rate}/s")

    try:
        asyncio.run(
            replay(
                file_path=file,
                rate=rate,
                shuffle_window=shuffle_window,
                duplicate_rate=duplicate_rate,
                drift_rate=drift_rate,
                update_timestamps=not no_update_timestamps,
                host=host,
                port=port,
            )
        )
    except KeyboardInterrupt:
        log.info("Replay interrupted")


if __name__ == "__main__":
    app()
