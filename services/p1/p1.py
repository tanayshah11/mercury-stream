import asyncio
import json
import os
import struct
import time

import orjson
import websockets
from pydantic import ValidationError

from shared import log, Ticker

P2_HOST = os.getenv("P2_HOST", "p2")
P2_PORT = int(os.getenv("P2_PORT", "9001"))
SYMBOL = os.getenv("SYMBOL", "BTC-USD")
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX", "10.0"))


def now_ms() -> int:
    return int(time.time() * 1000)


def frame(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


def build_subscribe_message(symbol: str) -> str:
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": [symbol],
            "channels": [{"name": "ticker", "product_ids": [symbol]}],
        }
    )


async def run() -> None:
    subscribe_msg = build_subscribe_message(SYMBOL)
    writer = None
    backoff_s = 1.0

    while True:
        try:
            if writer is None:
                _, writer = await asyncio.open_connection(P2_HOST, P2_PORT)
                log.info(f"Connected to P2 at {P2_HOST}:{P2_PORT}")

            async with websockets.connect(
                "wss://ws-feed.exchange.coinbase.com", ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(subscribe_msg)
                log.info(f"Subscribed to {SYMBOL}")
                backoff_s = 1.0

                while True:
                    response = await ws.recv()
                    try:
                        json_response = json.loads(response)
                    except json.JSONDecodeError as ex:
                        log.warning(f"WS JSON decode error: {ex}")
                        continue

                    if json_response.get("type") != "ticker":
                        continue

                    try:
                        ticker = Ticker.model_validate(json_response)
                    except ValidationError as e:
                        log.warning(f"Validation error: {e}")
                        continue

                    event = ticker.model_dump()
                    event["ingest_ts_ms"] = now_ms()
                    payload = orjson.dumps(event)

                    try:
                        writer.write(frame(payload))
                        await writer.drain()
                        backoff_s = 1.0
                    except (BrokenPipeError, ConnectionResetError, OSError) as ex:
                        raise OSError(f"IPC write failed: {ex}") from ex

        except (OSError, websockets.WebSocketException) as e:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
            writer = None

            log.warning(f"Connection failed ({e}); retrying in {backoff_s:.1f}s")
            await asyncio.sleep(backoff_s)
            backoff_s = min(backoff_s * 2, BACKOFF_MAX)


def main() -> None:
    log.info(f"Starting P1: symbol={SYMBOL} p2={P2_HOST}:{P2_PORT}")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
