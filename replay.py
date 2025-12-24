#!/usr/bin/env python3
"""
Replay tool for MercuryStream incident files and recorded data.

This tool is used to replay events from a JSONL file to a P2 service.
Usage:
    python replay.py --file data/btcusd.jsonl --rate 500
    python replay.py --file data/incidents/123/events.jsonl --rate 100 --shuffle-window 10 --duplicate-rate 0.05
"""

import argparse
import asyncio
import os
import random
import struct
import time

import orjson

from services.shared import log

P2_HOST = os.getenv("P2_HOST", "p2")
P2_PORT = int(os.getenv("P2_PORT", "9001"))


def now_ms() -> int:
    return int(time.time() * 1000)


def frame(payload: bytes) -> bytes:
    """Frame payload with 4-byte big-endian length prefix."""
    return struct.pack(">I", len(payload)) + payload


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.tokens = 0.0
        self.last_time = time.monotonic()

    async def acquire(self) -> None:
        if self.rate <= 0:
            return

        now = time.monotonic()
        elapsed = now - self.last_time
        self.tokens += elapsed * self.rate
        self.tokens = min(self.tokens, self.rate)
        self.last_time = now

        if self.tokens < 1.0:
            wait_time = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait_time)
            self.tokens = 0.0
        else:
            self.tokens -= 1.0


def apply_shuffle(events: list[dict], window_size: int) -> list[dict]:
    """Shuffle events within windows of size K."""
    if window_size <= 1:
        return events

    result = []
    for i in range(0, len(events), window_size):
        window = events[i : i + window_size]
        random.shuffle(window)
        result.extend(window)
    return result


def inject_duplicates(events: list[dict], rate: float) -> list[dict]:
    """Inject duplicate events at the given rate (0.0-1.0)."""
    if rate <= 0:
        return events

    result = []
    for event in events:
        result.append(event)
        if random.random() < rate:
            result.append(event.copy())
    return result


async def replay(
    file_path: str,
    rate: float = 0,
    shuffle_window: int = 0,
    duplicate_rate: float = 0.0,
    update_timestamps: bool = True,
    host: str = P2_HOST,
    port: int = P2_PORT,
) -> None:
    """Replay events from JSONL file to P2."""

    if not os.path.exists(file_path):
        log.error(f"File not found: {file_path}")
        return

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

    if shuffle_window > 0:
        log.info(f"Applying shuffle with window size {shuffle_window}")
        events = apply_shuffle(events, shuffle_window)

    if duplicate_rate > 0:
        original_count = len(events)
        events = inject_duplicates(events, duplicate_rate)
        log.info(f"Injected duplicates: {original_count} -> {len(events)} events")

    log.info(f"Connecting to P2 at {host}:{port}")
    try:
        _, writer = await asyncio.open_connection(host, port)
    except (OSError, ConnectionRefusedError) as e:
        log.error(f"Failed to connect to P2: {e}")
        return

    rate_limiter = RateLimiter(rate)
    sent = 0
    start_time = time.monotonic()

    try:
        for event in events:
            if update_timestamps:
                event["ingest_ts_ms"] = now_ms()

            payload = orjson.dumps(event)
            writer.write(frame(payload))
            await writer.drain()

            sent += 1
            if rate > 0:
                await rate_limiter.acquire()

            if sent % 1000 == 0:
                elapsed = time.monotonic() - start_time
                actual_rate = sent / elapsed if elapsed > 0 else 0
                log.info(f"Sent {sent}/{len(events)} events ({actual_rate:.1f}/s)")

    except (BrokenPipeError, ConnectionResetError) as e:
        log.error(f"Connection lost: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

    elapsed = time.monotonic() - start_time
    actual_rate = sent / elapsed if elapsed > 0 else 0
    log.info(f"Replay complete: {sent} events in {elapsed:.1f}s ({actual_rate:.1f}/s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay MercuryStream events")
    parser.add_argument(
        "--file", "-f", required=True, help="Path to JSONL file to replay"
    )
    parser.add_argument(
        "--rate",
        "-r",
        type=float,
        default=0,
        help="Events per second (0 = unlimited)",
    )
    parser.add_argument(
        "--shuffle-window",
        "-s",
        type=int,
        default=0,
        help="Shuffle events within windows of K events",
    )
    parser.add_argument(
        "--duplicate-rate",
        "-d",
        type=float,
        default=0.0,
        help="Rate of duplicate injection (0.0-1.0)",
    )
    parser.add_argument(
        "--no-update-timestamps",
        action="store_true",
        help="Don't update ingest_ts_ms to current time",
    )
    parser.add_argument("--host", default=P2_HOST, help=f"P2 host (default: {P2_HOST})")
    parser.add_argument(
        "--port", type=int, default=P2_PORT, help=f"P2 port (default: {P2_PORT})"
    )

    args = parser.parse_args()

    log.info(f"Starting replay: file={args.file} rate={args.rate}/s")

    try:
        asyncio.run(
            replay(
                file_path=args.file,
                rate=args.rate,
                shuffle_window=args.shuffle_window,
                duplicate_rate=args.duplicate_rate,
                update_timestamps=not args.no_update_timestamps,
                host=args.host,
                port=args.port,
            )
        )
    except KeyboardInterrupt:
        log.info("Replay interrupted")


if __name__ == "__main__":
    main()
