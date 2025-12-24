import asyncio
import os
import struct
import time

import orjson

from shared.logger import log
from consumer import consumer_health, consumer_slow, consumer_vwap
from forensics import consumer_forensics
from recorder import Recorder

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "9001"))
RECORD = os.getenv("RECORD", "false").lower() == "true"
RECORD_FILE = os.getenv("RECORD_FILE", "data/btcusd.jsonl")
ENABLE_SLOW = os.getenv("ENABLE_SLOW", "false").lower() == "true"
SLOW_DELAY_MS = int(os.getenv("SLOW_DELAY_MS", "50"))
FORENSICS = os.getenv("FORENSICS", "true").lower() == "true"
MAX_FRAME_LEN = 1_000_000


def now_ms() -> int:
    return int(time.time() * 1000)


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    length_bytes = await reader.readexactly(4)
    length = struct.unpack(">I", length_bytes)[0]
    if length > MAX_FRAME_LEN:
        raise ValueError(f"Frame length too large: {length} > {MAX_FRAME_LEN}")
    return await reader.readexactly(length)


class Bus:
    def __init__(self) -> None:
        self.subs: list[asyncio.Queue] = []
        self.drops = 0

    def subscribe(self, maxsize: int = 1000) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize)
        self.subs.append(q)
        return q

    def queue_depths(self) -> list[int]:
        return [q.qsize() for q in self.subs]

    def publish(self, event: dict) -> None:
        for q in self.subs:
            if q.full():
                try:
                    q.get_nowait()
                    self.drops += 1
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self.drops += 1


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    bus: Bus,
    recorder: Recorder | None = None,
) -> None:
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

            if "recv_ts_ms" not in event:
                event["recv_ts_ms"] = now_ms()

            if recorder is not None:
                recorder.record(event)

            bus.publish(event)

    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        log.error(f"Unexpected error in handle_client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        log.info(f"Client disconnected: {peer}")


async def run() -> None:
    bus = Bus()
    recorder = None

    if RECORD:
        recorder = Recorder(RECORD_FILE)
        await recorder.start()
        log.info(f"Recording enabled -> {RECORD_FILE}")

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, bus, recorder), HOST, PORT
    )
    log.info(f"P2 listening on {HOST}:{PORT}")

    consumers = [
        consumer_vwap(bus),
        consumer_health(bus),
    ]
    if FORENSICS:
        log.info("Forensics consumer enabled")
        consumers.append(consumer_forensics(bus))
    if ENABLE_SLOW:
        log.info(f"Slow consumer enabled (delay={SLOW_DELAY_MS}ms)")
        consumers.append(consumer_slow(bus, sleep_ms=SLOW_DELAY_MS))

    async with server:
        await asyncio.gather(server.serve_forever(), *consumers)


def main() -> None:
    log.info(f"Starting P2: {HOST}:{PORT}")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
