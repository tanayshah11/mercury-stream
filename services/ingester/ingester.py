import asyncio
import os
import struct
import time

import orjson
import websockets
from pydantic import ValidationError

from shared import log, Ticker

# Ingester: connects to Coinbase WebSocket and forwards to Processor
PROCESSOR_HOST = os.getenv("PROCESSOR_HOST", "processor")
PROCESSOR_PORT = int(os.getenv("PROCESSOR_PORT", "9001"))
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD").split(",") if s.strip()]
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX", "10.0"))
WS_RECV_TIMEOUT = float(os.getenv("WS_RECV_TIMEOUT", "30.0"))  # Seconds before recv() times out


def now_ms() -> int:
    return int(time.time() * 1000)


def frame(payload: bytes) -> bytes:
    # Length-prefixed framing: 4-byte big-endian length + payload
    return struct.pack(">I", len(payload)) + payload


def build_subscribe_message(symbols: list[str]) -> str:
    return orjson.dumps(
        {
            "type": "subscribe",
            "product_ids": symbols,
            "channels": [{"name": "ticker", "product_ids": symbols}],
        }
    ).decode("utf-8")


async def run() -> None:
    subscribe_msg = build_subscribe_message(SYMBOLS)
    writer = None
    backoff_s = 1.0

    while True:
        try:
            # Establish TCP connection to P2 for forwarding
            # Check both if writer is None AND if it's healthy (not closed)
            if writer is None or writer.is_closing():
                # Clean up old writer if it exists
                if writer is not None:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

                _, writer = await asyncio.open_connection(PROCESSOR_HOST, PROCESSOR_PORT)
                log.info(f"Connected to processor at {PROCESSOR_HOST}:{PROCESSOR_PORT}")

            # Connect to Coinbase WebSocket feed
            async with websockets.connect(
                "wss://ws-feed.exchange.coinbase.com", ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(subscribe_msg)
                log.info(f"Subscribed to {len(SYMBOLS)} symbols: {', '.join(SYMBOLS)}")
                backoff_s = 1.0

                while True:
                    # Add timeout to prevent hanging if Coinbase stops sending
                    try:
                        response = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
                    except TimeoutError:
                        log.warning(f"WebSocket recv timeout ({WS_RECV_TIMEOUT}s), reconnecting")
                        break  # Exit inner loop to reconnect

                    try:
                        json_response = orjson.loads(response)
                    except orjson.JSONDecodeError as ex:
                        log.warning(f"WS JSON decode error: {ex}")
                        continue

                    # Only process ticker events, ignore subscriptions/heartbeats
                    if json_response.get("type") != "ticker":
                        continue

                    # Validate against Ticker schema
                    try:
                        ticker = Ticker.model_validate(json_response)
                    except ValidationError as e:
                        log.warning(f"Validation error: {e}")
                        continue

                    # Add ingestion timestamp and feeder ID for forensics tracking
                    event = ticker.model_dump()
                    event["ingest_ts_ms"] = now_ms()
                    event["feeder_id"] = "coinbase"  # Identifies data source
                    payload = orjson.dumps(event)

                    # Forward framed message to P2 via TCP
                    try:
                        writer.write(frame(payload))
                        # Use wait_for with timeout to prevent unbounded memory buildup
                        await asyncio.wait_for(writer.drain(), timeout=5.0)
                        backoff_s = 1.0
                    except TimeoutError:
                        raise OSError("IPC drain timeout - TCP write buffer full") from None
                    except (BrokenPipeError, ConnectionResetError, OSError) as ex:
                        raise OSError(f"IPC write failed: {ex}") from ex

        except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as e:
            # Connection lost - cleanup writer and retry with exponential backoff
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
    # Entry point: start ingestion pipeline
    log.info(f"Starting ingester: symbols={SYMBOLS} processor={PROCESSOR_HOST}:{PROCESSOR_PORT}")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
