import asyncio
import os
import struct
import time

import orjson

from shared.logger import log
from consumer import (
    consumer_health,
    consumer_vwap,
    consumer_volatility,
    consumer_volume,
)
from forensics import consumer_forensics
from recorder import Recorder
from metrics import start_metrics_server, record_drop, set_queue_depth, METRICS_PORT

# Processor: receives from Ingester, fans out to consumers
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "9001"))
RECORD = os.getenv("RECORD", "false").lower() == "true"
RECORD_FILE = os.getenv("RECORD_FILE", "data/btcusd.jsonl")
FORENSICS = os.getenv("FORENSICS", "true").lower() == "true"
MAX_FRAME_LEN = 1_000_000


def now_ms() -> int:
    return int(time.time() * 1000)


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    # Decode length-prefixed frame from P1
    length_bytes = await reader.readexactly(4)
    length = struct.unpack(">I", length_bytes)[0]
    if length > MAX_FRAME_LEN:
        raise ValueError(f"Frame length too large: {length} > {MAX_FRAME_LEN}")
    return await reader.readexactly(length)


class Bus:
    # Fan-out event bus: publishes to multiple consumer queues
    def __init__(self) -> None:
        self.subs: list[asyncio.Queue] = []
        self.drops = 0

    def subscribe(self, maxsize: int = 1000) -> asyncio.Queue:
        # Consumers call this to get their own queue
        q: asyncio.Queue = asyncio.Queue(maxsize)
        self.subs.append(q)
        return q

    def queue_depths(self) -> list[int]:
        return [q.qsize() for q in self.subs]

    def max_queue_depth(self) -> int:
        depths = self.queue_depths()
        return max(depths) if depths else 0

    def publish(self, event: dict) -> None:
        # Non-blocking fanout: drop oldest if queue full to prevent blocking
        for q in self.subs:
            if q.full():
                try:
                    q.get_nowait()  # Drop oldest event
                    self.drops += 1
                    record_drop()
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self.drops += 1
                record_drop()


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    bus: Bus,
    recorder: Recorder | None = None,
) -> None:
    # Handle incoming TCP connection from P1
    peer = writer.get_extra_info("peername")
    log.info(f"Client connected: {peer}")

    try:
        while True:
            try:
                payload = await read_frame(reader)
            except ValueError as e:
                log.warning(f"Frame error: {e}")
                continue

            try:
                event = orjson.loads(payload)
            except orjson.JSONDecodeError as e:
                log.warning(f"JSON decode error: {e}")
                continue

            if not isinstance(event, dict):
                log.warning(f"Invalid event type: {type(event)}")
                continue

            # Add receive timestamp if not present
            if "recv_ts_ms" not in event:
                event["recv_ts_ms"] = now_ms()

            # Optional: persist to disk
            if recorder is not None:
                recorder.record(event)

            # Fan out to all consumer queues
            bus.publish(event)

    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        log.error(f"Unexpected error in handle_client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        log.info(f"Client disconnected: {peer}")


async def metrics_updater(bus: Bus, interval_s: float = 1.0) -> None:
    # Export queue depth metrics for Prometheus scraping
    while True:
        set_queue_depth(bus.max_queue_depth())
        await asyncio.sleep(interval_s)


async def run() -> None:
    bus = Bus()
    recorder = None

    # Start Prometheus metrics endpoint
    start_metrics_server(METRICS_PORT)
    log.info(f"Metrics server started on port {METRICS_PORT}")

    # Optional: enable event recording to disk
    if RECORD:
        recorder = Recorder(RECORD_FILE)
        await recorder.start()
        log.info(f"Recording enabled -> {RECORD_FILE}")

    # TCP server accepting connections from P1
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, bus, recorder), HOST, PORT
    )
    log.info(f"P2 listening on {HOST}:{PORT}")

    # Spawn consumer tasks that process events from the bus
    consumers = [
        consumer_vwap(bus),
        consumer_health(bus),
        consumer_volatility(bus),
        consumer_volume(bus),
        metrics_updater(bus),
    ]
    if FORENSICS:
        log.info("Forensics consumer enabled")
        consumers.append(consumer_forensics(bus))

    # Run server and all consumers concurrently
    async with server:
        await asyncio.gather(server.serve_forever(), *consumers)


def main() -> None:
    # Entry point: start event distribution server
    log.info(f"Starting processor: {HOST}:{PORT}")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
